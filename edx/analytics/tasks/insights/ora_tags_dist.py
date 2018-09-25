import logging
import luigi

import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.util.url import get_target_from_url
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util.record import Record, StringField, IntegerField
from edx.analytics.tasks.insights.tags_dist import TagsDistributionDownstreamMixin


log = logging.getLogger(__name__)


class OraTagsDistributionPerCourse(
        TagsDistributionDownstreamMixin,
        EventLogSelectionMixin,
        MapReduceJobTask):
    """Calculates ora tags distribution."""

    def output(self):
        return get_target_from_url(self.output_root)

    def mapper(self, line):
        """
        Args:
            line: text line from a tracking event log.

        Yields:  (course_id, org_id, problem_id), (timestamp, saved_tags, is_correct)

        """
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _ = value

        event_type = event.get('event_type')
        ora_event_types = {'openassessmentblock.staff_assess': 'staff',
                           'openassessmentblock.self_assess': 'self',
                           'openassessmentblock.peer_assess': 'peer'}

        assessment_type = ora_event_types.get(event_type, None)

        if not assessment_type or event.get('event_source') != 'server':
            return

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        org_id = opaque_key_util.get_org_id_for_course(course_id)

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        ora_id = event.get('context').get('module', {}).get('usage_key')
        if not ora_id:
            return

        saved_tags = event.get('context').get('asides', {}).get('tagging_ora_aside', {}).get('saved_tags', {})

        parts = event.get('event', {}).get('parts', [])
        for part in parts:
            part_criterion_name = part.get('criterion', {}).get('name', None)
            part_points_possible = int(part.get('criterion', {}).get('points_possible', 0))
            part_points_scored = int(part.get('option', {}).get('points', 0))
            part_saved_tags = saved_tags.get(part_criterion_name, {})

            yield (course_id, org_id, ora_id, assessment_type, part_criterion_name),\
                  (timestamp, part_saved_tags, part_points_possible, part_points_scored)

    def reducer(self, key, values):
        """
        Args:
            key:  (course_id, org_id, ora_id, assessment_type, criterion_name)
            values:  iterator of (timestamp, saved_tags, points_possible, points_scored)

        """

        course_id, org_id, ora_id, assessment_type, criterion_name = key

        total_earned_points = 0
        num_submissions_count = 0

        latest_timestamp = None
        latest_tags = None
        latest_points_possible = None

        for timestamp, saved_tags, points_possible, points_scored in values:
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
                latest_tags = saved_tags.copy() if saved_tags else None
                latest_points_possible = points_possible

            total_earned_points += points_scored
            num_submissions_count += 1

        if not latest_tags:
            yield OraTagsDistributionRecord(
                course_id=course_id,
                org_id=org_id,
                module_id=ora_id,
                criterion_name=criterion_name,
                assessment_type=assessment_type,
                tag_name=None,
                tag_value=None,
                possible_points=latest_points_possible,
                total_earned_points=total_earned_points,
                submissions_count=num_submissions_count).to_string_tuple()
        else:
            for tag_key, tag_val in latest_tags.iteritems():
                tag_val_lst = [tag_val] if isinstance(tag_val, basestring) else tag_val
                for val in tag_val_lst:
                    yield OraTagsDistributionRecord(
                        course_id=course_id,
                        org_id=org_id,
                        module_id=ora_id,
                        criterion_name=criterion_name,
                        assessment_type=assessment_type,
                        tag_name=tag_key,
                        tag_value=val,
                        possible_points=latest_points_possible,
                        total_earned_points=total_earned_points,
                        submissions_count=num_submissions_count).to_string_tuple()


class OraTagsDistributionRecord(Record):
    course_id = StringField(length=255, nullable=False, description='Course id')
    org_id = StringField(length=255, nullable=False, description='Org id')
    module_id = StringField(length=255, nullable=False, description='Ora id')
    criterion_name = StringField(length=255, nullable=False, description='Criterion name')
    assessment_type = StringField(length=255, nullable=False, description='Assessment type')
    tag_name = StringField(length=255, nullable=True, description='Tag key')
    tag_value = StringField(length=255, nullable=True, description='Tag value')
    possible_points = IntegerField(nullable=False, description='Possible points')
    total_earned_points = IntegerField(nullable=False, description='Total earned points')
    submissions_count = IntegerField(nullable=False, description='Submissions count')


@workflow_entry_point
class OraTagsDistributionWorkflow(
        TagsDistributionDownstreamMixin,
        EventLogSelectionDownstreamMixin,
        MapReduceJobTaskMixin,
        MysqlInsertTask,
        luigi.WrapperTask):

    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BoolParameter(
        default=True,
        description="Whether or not to overwrite existing outputs"
    )

    @property
    def insert_source_task(self):
        """
        Write to ora_tags_distribution table.
        """
        return OraTagsDistributionPerCourse(
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.output_root,
            interval=self.interval,
            source=self.source
        )

    @property
    def table(self):
        return "ora_tags_distribution"

    @property
    def columns(self):
        return OraTagsDistributionRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('module_id',),
            ('course_id', 'module_id'),
        ]
