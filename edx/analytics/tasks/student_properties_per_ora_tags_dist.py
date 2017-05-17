import logging
import luigi
import luigi.hdfs
import luigi.s3

import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util.record import Record, StringField, IntegerField
from edx.analytics.tasks.student_properties_per_tags_dist import StudentPropertiesPerTagsPerCourseDownstreamMixin


log = logging.getLogger(__name__)


class StudentPropertiesPerOraTagsPerCourse(
        StudentPropertiesPerTagsPerCourseDownstreamMixin,
        EventLogSelectionMixin,
        MapReduceJobTask):

    def output(self):
        return get_target_from_url(self.output_root)

    def mapper(self, line):
        """
        Args:
            line: text line from a tracking event log.

        Yields:  (course_id, org_id, course, run, problem_id), (timestamp, saved_tags, student_properties, is_correct)

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
        course = opaque_key_util.get_course_for_course(course_id)
        run = opaque_key_util.get_run_for_course(course_id)

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        ora_id = event.get('context').get('module', {}).get('usage_key')
        if not ora_id:
            return

        saved_tags = event.get('context').get('asides', {}).get('tagging_ora_aside', {}).get('saved_tags', {})
        student_properties = event.get('context').get('asides', {}).get('student_properties_aside', {})\
            .get('student_properties', {})

        parts = event.get('event', {}).get('parts', [])
        for part in parts:
            part_criterion_name = part.get('criterion', {}).get('name', None)
            part_points_possible = int(part.get('criterion', {}).get('points_possible', 0))
            part_points_scored = int(part.get('option', {}).get('points', 0))
            part_saved_tags = saved_tags.get(part_criterion_name, {})

            yield (course_id, org_id, course, run, ora_id, assessment_type, part_criterion_name),\
                  (timestamp, part_saved_tags, student_properties, part_points_possible, part_points_scored)

    def reducer(self, key, values):
        """
        Args:
            key:  (course_id, org_id, course, run, ora_id, assessment_type, criterion_name)
            values:  iterator of (timestamp, saved_tags, student_properties, points_possible, points_scored)
            
        """
        course_id, org_id, course, run, ora_id, assessment_type, criterion_name = key

        total_earned_points = 0
        num_submissions_count = 0

        latest_timestamp = None
        latest_tags = None
        latest_points_possible = None
        props = {'registration': {}, 'enrollment': {}}

        # prepare base dicts for tags and properties

        for timestamp, saved_tags, student_properties, points_possible, points_scored in values:
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
                latest_tags = saved_tags.copy() if saved_tags else None
                latest_points_possible = points_possible

            total_earned_points += points_scored
            num_submissions_count += 1

            for prop_type, prop_dict in student_properties.iteritems():
                for prop_name, prop_value in prop_dict.iteritems():
                    if prop_name not in props[prop_type]:
                        props[prop_type][prop_name] = {}
                    if prop_value not in props[prop_type][prop_name]:
                        props[prop_type][prop_name][prop_value] = {
                            'total_earned_points': 0,
                            'num_submissions_count': 0
                        }
                    props[prop_type][prop_name][prop_value]['total_earned_points'] += points_scored
                    props[prop_type][prop_name][prop_value]['num_submissions_count'] += 1

        # convert properties dict to the list

        props_list_values = []
        for prop_type, prop_dict in props.iteritems():
            for prop_name, prop_value_dict in prop_dict.iteritems():
                for prop_value, prop_nums in prop_value_dict.iteritems():
                    props_list_values.append({
                        'type': prop_type,
                        'name': prop_name,
                        'value': prop_value,
                        'total_earned_points': prop_nums['total_earned_points'],
                        'num_submissions_count': prop_nums['num_submissions_count'],
                    })

        # convert latest tags dict to extended dict. Example:
        # { 'lo': ['AAC&U VALUE Rubric - Written Communication - Genre and Disciplinary Conventions',
        #          'Paul & Elder Critical Thinking Model - Concepts and Ideas'] }
        # =>
        # { 'lo': ['AAC&U VALUE Rubric', 'AAC&U VALUE Rubric - Written Communication',
        #          'AAC&U VALUE Rubric - Written Communication - Genre and Disciplinary Conventions',
        #          'Paul & Elder Critical Thinking Model'
        #          'Paul & Elder Critical Thinking Model - Concepts and Ideas'] }

        tags_extended_dict = {}
        if latest_tags:
            for tag_key, tag_val in latest_tags.iteritems():
                tag_val_lst = [tag_val] if isinstance(tag_val, basestring) else tag_val
                tags_extended_dict[tag_key] = []
                for tag in tag_val_lst:
                    tag_split_lst = tag.split(' - ')
                    for idx, tag_part in enumerate(tag_split_lst):
                        tag_new_val = ' - '.join(tag_split_lst[0:idx + 1])
                        if tag_new_val not in tags_extended_dict[tag_key]:
                            tags_extended_dict[tag_key].append(tag_new_val)

        # save values to the database table

        if not latest_tags:
            for prop_val in props_list_values:
                yield StudentPropertiesAndOraTagsRecord(
                    course_id=course_id,
                    org_id=org_id,
                    course=course,
                    run=run,
                    module_id=ora_id,
                    criterion_name=criterion_name,
                    assessment_type=assessment_type,
                    property_type=prop_val['type'],
                    property_name=prop_val['name'],
                    property_value=prop_val['value'],
                    tag_name=None,
                    tag_value=None,
                    possible_points=latest_points_possible,
                    total_earned_points=prop_val['total_earned_points'],
                    submissions_count=prop_val['num_submissions_count']).to_string_tuple()
        else:
            for tag_key, tags_extended_lst in tags_extended_dict.iteritems():
                for val in tags_extended_lst:
                    yield StudentPropertiesAndOraTagsRecord(
                        course_id=course_id,
                        org_id=org_id,
                        course=course,
                        run=run,
                        module_id=ora_id,
                        criterion_name=criterion_name,
                        assessment_type=assessment_type,
                        property_type=None,
                        property_name=None,
                        property_value=None,
                        tag_name=tag_key,
                        tag_value=val,
                        possible_points=latest_points_possible,
                        total_earned_points=total_earned_points,
                        submissions_count=num_submissions_count).to_string_tuple()
                    for prop_val in props_list_values:
                        yield StudentPropertiesAndOraTagsRecord(
                            course_id=course_id,
                            org_id=org_id,
                            course=course,
                            run=run,
                            module_id=ora_id,
                            criterion_name=criterion_name,
                            assessment_type=assessment_type,
                            property_type=prop_val['type'],
                            property_name=prop_val['name'],
                            property_value=prop_val['value'],
                            tag_name=tag_key,
                            tag_value=val,
                            possible_points=latest_points_possible,
                            total_earned_points=prop_val['total_earned_points'],
                            submissions_count=prop_val['num_submissions_count']).to_string_tuple()


class StudentPropertiesAndOraTagsRecord(Record):
    course_id = StringField(length=255, nullable=False, description='Course id')
    org_id = StringField(length=255, nullable=False, description='Org id')
    course = StringField(length=255, nullable=False, description='Course')
    run = StringField(length=255, nullable=False, description='Run')
    module_id = StringField(length=255, nullable=False, description='Ora id')
    criterion_name = StringField(length=255, nullable=False, description='Criterion name')
    assessment_type = StringField(length=255, nullable=False, description='Assessment type')
    property_type = StringField(length=255, nullable=True, description='Property type')
    property_name = StringField(length=255, nullable=True, description='Property name')
    property_value = StringField(length=255, nullable=True, description='Property value')
    tag_name = StringField(length=255, nullable=True, description='Tag key')
    tag_value = StringField(length=255, nullable=True, description='Tag value')
    possible_points = IntegerField(nullable=False, description='Possible points')
    total_earned_points = IntegerField(nullable=False, description='Total earned points')
    submissions_count = IntegerField(nullable=False, description='Submissions count')


@workflow_entry_point
class StudentPropertiesAndOraTagsDistributionWorkflow(
        StudentPropertiesPerTagsPerCourseDownstreamMixin,
        EventLogSelectionDownstreamMixin,
        MapReduceJobTaskMixin,
        MysqlInsertTask,
        luigi.WrapperTask):

    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BooleanParameter(
        default=True,
        description="Whether or not to overwrite existing outputs"
    )

    @property
    def insert_source_task(self):
        """
        Write to ora_tags_distribution table.
        """
        return StudentPropertiesPerOraTagsPerCourse(
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.output_root,
            interval=self.interval,
            source=self.source
        )

    @property
    def table(self):
        return "student_properties_and_ora_tags"

    @property
    def columns(self):
        return StudentPropertiesAndOraTagsRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',),
        ]
