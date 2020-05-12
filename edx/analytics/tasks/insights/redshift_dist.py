import hashlib
import logging
import luigi
import json

from edx.analytics.tasks.util.url import get_target_from_url
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util.record import Record, StringField, IntegerField, FloatField, BooleanField, DateTimeField
from edx.analytics.tasks.insights.event_parser import EventProcessor
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask


log = logging.getLogger(__name__)


class RedShiftDownstreamMixin(object):
    output_root = luigi.Parameter(
        description='Directory to store the output in.',
    )


class RedShiftBaseTask(RedShiftDownstreamMixin, EventLogSelectionMixin, MapReduceJobTask):
    EVENT_TYPES = [
        'problem_check',
        'edx.drag_and_drop_v2.item.dropped',
        'openassessmentblock.create_submission',
        'openassessmentblock.staff_assess',
        'sequential_block.viewed',
        'sequential_block.remove_view',
        'xblock.image-explorer.hotspot.opened',
    ]

    def output(self):
        return get_target_from_url(self.output_root)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _ = value

        event_type = event.get('event_type')

        if event.get('event_source') != 'server' or event_type not in self.EVENT_TYPES:
            return

        res = EventProcessor.process(event_type, event)
        if not res:
            return

        if not isinstance(res, list):
            res = [res]

        for e in res:
            yield (e.course_id, e.org_id, e.course, e.run, e.term, e.block_id, e.user_id,
                   e.ora_block, e.criterion_name),\
                  (event_type, e.timestamp, e.display_name, e.question_text,
                   e.grade, e.max_grade, e.answers, e.correctness, e.is_new_attempt)

    def reducer(self, key, values):
        course_id, org_id, course, run, term, block_id, user_id, ora_block, ora_criterion_name = key

        updated_values = [v for v in values]  # gen -> list

        attempts = []
        len_values = len(updated_values)
        if len_values > 1:
            updated_values = sorted(updated_values, key=lambda tup: tup[1])  # sort by timestamp

        if len(updated_values) > 1:
            unique_event_types = list(set([v[0] for v in updated_values]))
            if len(unique_event_types) == 2 \
                    and updated_values[0][0] == 'sequential_block.viewed' \
                    and updated_values[-1][0] == 'sequential_block.remove_view':
                return

        answer_id = str(user_id) + '-' + block_id
        question_token = block_id if not ora_block else block_id + '-' + ora_criterion_name
        question_hash = hashlib.md5(question_token.encode('utf-8')).hexdigest()

        num = 1
        for event_type, timestamp, display_name, question_text, grade, max_grade, answer, correctness, is_new_attempt\
                in updated_values:
            question_name = display_name if not ora_block else ora_criterion_name
            if is_new_attempt:
                attempts.append({
                    'grade': grade,
                    'answer': answer,
                    'timestamp': timestamp,
                    'correctness': correctness
                })

            if num == len_values:
                attempts_json = json.dumps(attempts) if attempts else None
                user_id_val = int(user_id) if user_id else None
                dt = DateTimeField().deserialize_from_string(timestamp)
                yield RedShiftRecord(
                    course_id=course_id,
                    org_id=org_id,
                    course=course,
                    run=run,
                    prop_term=term,
                    block_id=block_id,
                    user_id=user_id_val,
                    answer_id=answer_id,
                    timestamp=dt,
                    display_name=display_name,
                    question_name=question_name,
                    question_text=question_text,
                    question_hash=question_hash,
                    is_ora_block=ora_block,
                    ora_criterion_name=ora_criterion_name,
                    grade=grade,
                    max_grade=max_grade,
                    answer=answer,
                    correctness=correctness,
                    attempts=attempts_json
                ).to_string_tuple()
            else:
                num = num + 1


class RedShiftRecord(Record):
    course_id = StringField(length=255, nullable=False, description='Course ID')
    org_id = StringField(length=80, nullable=False, description='Org ID')
    course = StringField(length=255, nullable=False, description='Course')
    run = StringField(length=80, nullable=False, description='Run')
    prop_term = StringField(length=20, nullable=True, description='Term')
    block_id = StringField(length=255, nullable=False, description='Problem ID')
    user_id = IntegerField(nullable=False, description='User ID')
    answer_id = StringField(length=255, nullable=False, description='Answer ID')
    timestamp = DateTimeField(nullable=False, description='Event timestamp')
    display_name = StringField(length=2048, nullable=True, description='Problem Display Name')
    question_name = StringField(length=2048, nullable=True, description='Question Name')
    question_text = StringField(length=65535, nullable=True, description='Question Text')
    question_hash = StringField(length=80, nullable=True, description='Question Hash')
    is_ora_block = BooleanField(default=False, nullable=False, description='True if the block is a ORA question')
    ora_criterion_name = StringField(length=255, nullable=True, description='ORA criterion name')
    grade = FloatField(nullable=True, description='Grade')
    max_grade = FloatField(nullable=True, description='Max grade')
    answer = StringField(length=65535, nullable=True, description='Answer')
    correctness = StringField(length=20, nullable=True, description='Correctness')
    attempts = StringField(length=65535, nullable=True, description='Attempts')


@workflow_entry_point
class RedShiftDistributionWorkflow(RedShiftDownstreamMixin,
                                   EventLogSelectionDownstreamMixin,
                                   MapReduceJobTaskMixin,
                                   MysqlInsertTask,
                                   luigi.WrapperTask):

    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BoolParameter(
        default=True,
        description="Whether or not to overwrite existing outputs",
        significant=False
    )

    @property
    def insert_source_task(self):
        return RedShiftBaseTask(
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.output_root,
            interval=self.interval,
            source=self.source
        )

    @property
    def table(self):
        return "tracking_events"

    @property
    def columns(self):
        return RedShiftRecord.get_sql_schema()

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return []

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('org_id',),
            ('block_id',),
            ('user_id',),
            ('org_id', 'timestamp'),
        ]
