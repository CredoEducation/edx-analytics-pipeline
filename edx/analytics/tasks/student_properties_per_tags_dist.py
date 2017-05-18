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


log = logging.getLogger(__name__)


class StudentPropertiesPerTagsPerCourseDownstreamMixin(object):
    """
    Base class for tags distribution calculations.

    """
    output_root = luigi.Parameter(
        description='Directory to store the output in.',
    )


class StudentPropertiesPerTagsPerCourse(StudentPropertiesPerTagsPerCourseDownstreamMixin,
                                        EventLogSelectionMixin,
                                        MapReduceJobTask):
    """Calculates tags distribution."""

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

        if event.get('event_type') != 'problem_check' or event.get('event_source') != 'server':
            return

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        if opaque_key_util.ignore_erie_admin_events(course_id, eventlog.get_user_id(event)):
            return

        org_id = opaque_key_util.get_org_id_for_course(course_id)
        course = opaque_key_util.get_course_for_course(course_id)
        run = opaque_key_util.get_run_for_course(course_id)

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        problem_id = event_data.get('problem_id')
        if not problem_id:
            return

        is_correct = event_data.get('success') == 'correct'

        saved_tags = event.get('context').get('asides', {}).get('tagging_aside', {}).get('saved_tags', {})
        student_properties = event.get('context').get('asides', {}).get('student_properties_aside', {})\
            .get('student_properties', {})

        yield (course_id, org_id, course, run, problem_id), (timestamp, saved_tags, student_properties, is_correct)

    def reducer(self, key, values):
        """
        Calculate the count of total/correct submissions for each pair:
        problem + related tag + related students properties

        Args:
            key:  (course_id, org_id, course, run, problem_id)
            values:  iterator of (timestamp, saved_tags, student_properties, is_correct)

        """
        course_id, org_id, course, run, problem_id = key

        num_correct = 0
        num_total = 0

        latest_timestamp = None
        latest_tags = None
        props = {'registration': {}, 'enrollment': {}}

        # prepare base dicts for tags and properties

        for timestamp, saved_tags, student_properties, is_correct in values:
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
                latest_tags = saved_tags.copy() if saved_tags else None

            if is_correct:
                num_correct += 1

            num_total += 1

            for prop_type, prop_dict in student_properties.iteritems():
                for prop_name, prop_value in prop_dict.iteritems():
                    if prop_name not in props[prop_type]:
                        props[prop_type][prop_name] = {}
                    if prop_value not in props[prop_type][prop_name]:
                        props[prop_type][prop_name][prop_value] = {
                            'num_correct': 0,
                            'num_total': 0
                        }
                    if is_correct:
                        props[prop_type][prop_name][prop_value]['num_correct'] += 1
                    props[prop_type][prop_name][prop_value]['num_total'] += 1

        # convert properties dict to the list

        props_list_values = []
        for prop_type, prop_dict in props.iteritems():
            for prop_name, prop_value_dict in prop_dict.iteritems():
                for prop_value, prop_nums in prop_value_dict.iteritems():
                    props_list_values.append({
                        'type': prop_type,
                        'name': prop_name,
                        'value': prop_value,
                        'num_total': prop_nums['num_total'],
                        'num_correct': prop_nums['num_correct'],
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
                yield StudentPropertiesAndTagsRecord(
                    course_id=course_id,
                    org_id=org_id,
                    course=course,
                    run=run,
                    module_id=problem_id,
                    property_type=prop_val['type'],
                    property_name=prop_val['name'],
                    property_value=prop_val['value'],
                    tag_name=None,
                    tag_value=None,
                    total_submissions=prop_val['num_total'],
                    correct_submissions=prop_val['num_correct']).to_string_tuple()
        else:
            for tag_key, tags_extended_lst in tags_extended_dict.iteritems():
                for val in tags_extended_lst:
                    yield StudentPropertiesAndTagsRecord(
                        course_id=course_id,
                        org_id=org_id,
                        course=course,
                        run=run,
                        module_id=problem_id,
                        property_type=None,
                        property_name=None,
                        property_value=None,
                        tag_name=tag_key,
                        tag_value=val,
                        total_submissions=num_total,
                        correct_submissions=num_correct).to_string_tuple()
                    for prop_val in props_list_values:
                        yield StudentPropertiesAndTagsRecord(
                            course_id=course_id,
                            org_id=org_id,
                            course=course,
                            run=run,
                            module_id=problem_id,
                            property_type=prop_val['type'],
                            property_name=prop_val['name'],
                            property_value=prop_val['value'],
                            tag_name=tag_key,
                            tag_value=val,
                            total_submissions=prop_val['num_total'],
                            correct_submissions=prop_val['num_correct']).to_string_tuple()


class StudentPropertiesAndTagsRecord(Record):
    course_id = StringField(length=255, nullable=False, description='Course id')
    org_id = StringField(length=255, nullable=False, description='Org id')
    course = StringField(length=255, nullable=False, description='Course')
    run = StringField(length=255, nullable=False, description='Run')
    module_id = StringField(length=255, nullable=False, description='Problem id')
    property_type = StringField(length=255, nullable=True, description='Property type')
    property_name = StringField(length=255, nullable=True, description='Property name')
    property_value = StringField(length=255, nullable=True, description='Property value')
    tag_name = StringField(length=255, nullable=True, description='Tag key')
    tag_value = StringField(length=255, nullable=True, description='Tag value')
    total_submissions = IntegerField(nullable=False, description='Number of total submissions')
    correct_submissions = IntegerField(nullable=False, description='Number of correct submissions')


@workflow_entry_point
class StudentPropertiesAndTagsDistributionWorkflow(StudentPropertiesPerTagsPerCourseDownstreamMixin,
                                                   EventLogSelectionDownstreamMixin,
                                                   MapReduceJobTaskMixin,
                                                   MysqlInsertTask,
                                                   luigi.WrapperTask):

    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BooleanParameter(
        default=True,
        description="Whether or not to overwrite existing outputs",
        significant=False
    )

    @property
    def insert_source_task(self):
        return StudentPropertiesPerTagsPerCourse(
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.output_root,
            interval=self.interval,
            source=self.source
        )

    @property
    def table(self):
        return "student_properties_and_tags"

    @property
    def columns(self):
        return StudentPropertiesAndTagsRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('tag_value', 'property_name', 'property_value'),
        ]
