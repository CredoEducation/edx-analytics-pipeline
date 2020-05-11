import logging
import luigi

from edx.analytics.tasks.util.url import get_target_from_url
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util.record import Record, StringField
from edx.analytics.tasks.insights.event_parser import EventProcessor
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.insights.redshift_dist import RedShiftDownstreamMixin


log = logging.getLogger(__name__)


class RedShiftPropsBaseTask(RedShiftDownstreamMixin, EventLogSelectionMixin, MapReduceJobTask):
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
                  (event_type, e.timestamp, e.student_properties)

    def reducer(self, key, values):
        course_id, org_id, course, run, term, block_id, user_id, ora_block, ora_criterion_name = key

        updated_values = [v for v in values]  # gen -> list

        len_values = len(updated_values)
        if len_values > 1:
            updated_values = sorted(updated_values, key=lambda tup: tup[1])  # sort by timestamp

        answer_id = str(user_id) + '-' + block_id

        num = 1
        for event_type, timestamp, student_properties in updated_values:
            if num == len_values:
                for prop_name, prop_value in student_properties.items():
                    if len(prop_value) > 255:
                        prop_value = prop_value[0:255]
                    yield RedShiftPropRecord(
                        answer_id=answer_id,
                        prop_name=prop_name,
                        prop_value=prop_value
                    ).to_string_tuple()
            else:
                num = num + 1


class RedShiftPropRecord(Record):
    answer_id = StringField(length=255, nullable=False, description='Answer ID')
    prop_name = StringField(length=255, nullable=False, description='Property Name')
    prop_value = StringField(length=255, nullable=False, description='Property Name')


@workflow_entry_point
class RedShiftPropsDistributionWorkflow(
    RedShiftDownstreamMixin,
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
        return RedShiftPropsBaseTask(
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.output_root,
            interval=self.interval,
            source=self.source
        )

    @property
    def table(self):
        return "tracking_events_props"

    @property
    def columns(self):
        return RedShiftPropRecord.get_sql_schema()

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return []

    @property
    def indexes(self):
        return [
            ('answer_id',),
            ('prop_name',),
        ]
