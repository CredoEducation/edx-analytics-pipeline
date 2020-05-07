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
#        'xblock.image-explorer.hotspot.opened',
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
            yield (e.course_id, e.org_id, e.course, e.run, e.term, e.block_id, e.user_id, e.ora_block, e.criterion_name),\
                  (event_type, e.timestamp, e.display_name, e.question_text, e.question_hash, e.student_properties,
                   e.saved_tags, e.grade, e.answers, e.correctness, e.is_new_attempt)

    def reducer(self, key, values):
        course_id, org_id, course, run, term, block_id, user_id, ora_block, ora_criterion_name = key

        updated_values = [v for v in values]  # gen -> list

        attempts = []
        len_values = len(updated_values)
        if len_values > 1:
            updated_values = sorted(updated_values, key=lambda tup: tup[1])  # sort by timestamp

        num = 1
        for event_type, timestamp, display_name, question_text, question_hash, student_properties,\
            saved_tags, grade, answers, correctness, is_new_attempt in updated_values:
            if is_new_attempt:
                attempts.append({
                    'grade': grade,
                    'answers': answers,
                    'timestamp': timestamp,
                    'correctness': correctness
                })

            if num == len_values:
                properties_data_json = json.dumps(student_properties) if student_properties else None
                tags_json = json.dumps(saved_tags) if saved_tags else None
                attempts_json = json.dumps(attempts) if attempts else None
                user_id_val = int(user_id) if user_id else None
                dt = DateTimeField().deserialize_from_string(timestamp)
                yield RedShiftRecord(
                    course_id=course_id,
                    org_id=org_id,
                    course=course,
                    run=run,
                    prop_term=term,
                    module_id=block_id,
                    user_id=user_id_val,
                    timestamp=dt,
                    display_name=display_name,
                    question_text=question_text,
                    name_hash=question_hash,
                    is_ora_block=ora_block,
                    ora_criterion_name=ora_criterion_name,
                    properties_data=properties_data_json,
                    tags=tags_json,
                    grade=grade,
                    answers=answers,
                    correctness=correctness,
                    attempts=attempts_json
                ).to_string_tuple()
            else:
                num = num + 1


class RedShiftRecord(Record):
    course_id = StringField(length=255, nullable=False, description='Course id')
    org_id = StringField(length=80, nullable=False, description='Org id')
    course = StringField(length=255, nullable=False, description='Course')
    run = StringField(length=80, nullable=False, description='Run')
    prop_term = StringField(length=20, nullable=True, description='Term')
    module_id = StringField(length=255, nullable=False, description='Problem id')
    user_id = IntegerField(nullable=False, description='User ID')
    timestamp = DateTimeField(nullable=False, description='Event timestamp')
    display_name = StringField(length=2048, nullable=True, description='Problem Display Name')
    question_text = StringField(length=65535, nullable=True, description='Question Text')
    name_hash = StringField(length=80, nullable=True, description='Name Hash')
    is_ora_block = BooleanField(default=False, nullable=False, description='True if the block is a ORA question')
    ora_criterion_name = StringField(length=255, nullable=True, description='ORA criterion name')
    properties_data = StringField(length=65535, nullable=True, description='Properties data in JSON format')
    tags = StringField(length=4096, nullable=True, description='Tags')
    grade = FloatField(nullable=True, description='Grade')
    answers = StringField(length=65535, nullable=True, description='Answer')
    correctness = StringField(length=15, nullable=True, description='Correctness')
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
            ('module_id',),
            ('user_id',),
            ('timestamp',),
        ]
