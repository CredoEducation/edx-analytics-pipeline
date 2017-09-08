import logging
import luigi
import luigi.hdfs
import luigi.s3
import hashlib
import json

from collections import defaultdict

import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util.record import Record, StringField, IntegerField
from edx.analytics.tasks.student_properties_per_tags_dist import StudentPropertiesPerTagsPerCourseDownstreamMixin, \
    get_value_from_student_properties


log = logging.getLogger(__name__)


class StudentPropertiesPerOraTagsPerCourse(
        StudentPropertiesPerTagsPerCourseDownstreamMixin,
        EventLogSelectionMixin,
        MapReduceJobTask):

    def output(self):
        return get_target_from_url(self.output_root)

    def _dist_earned_points_info(self, points):
        dist = defaultdict(int)
        result = []
        for p in points:
            dist[(p['points'], p['name'])] += 1
        for r in dist:
            points, name = r
            result.append({
                'points': points,
                'name': name,
                'count': dist[r]
            })
        return result

    def _sum_earned_points(self, points):
        return sum([p['points'] for p in points])

    def mapper(self, line):
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

        if opaque_key_util.ignore_erie_admin_events(course_id, eventlog.get_user_id(event)):
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

        overload_items = {'course': course, 'term': run}
        for k in overload_items:
            new_value, new_properties = get_value_from_student_properties(k, student_properties)
            if new_value:
                overload_items[k], student_properties = new_value, new_properties

        question_text = u''
        prompts_list = []
        prompts = event.get('event', {}).get('prompts', [])
        if prompts:
            for prompt in prompts:
                if 'description' in prompt:
                    prompts_list.append(prompt['description'])

        if prompts_list:
            question_text = u". ".join(prompts_list)
        question_text = question_text.replace("\n", " ")

        parts = event.get('event', {}).get('parts', [])
        for part in parts:
            part_criterion_name = part.get('criterion', {}).get('name', None)
            part_points_possible = int(part.get('criterion', {}).get('points_possible', 0))
            part_points_scored = part.get('option', {})
            part_saved_tags = saved_tags.get(part_criterion_name, {})

            yield (course_id, org_id, overload_items['course'], overload_items['term'], ora_id, assessment_type, part_criterion_name),\
                  (timestamp, part_saved_tags, student_properties, part_points_possible, part_points_scored,
                   question_text)

    def reducer(self, key, values):
        course_id, org_id, course, run, ora_id, assessment_type, criterion_name = key

        total_earned_points_info = []
        num_submissions_count = 0

        latest_timestamp = None
        latest_question_text = u''
        latest_tags = None
        latest_points_possible = None

        props = []
        props_info = []
        props_json = None

        # prepare base dicts for tags and properties

        for timestamp, saved_tags, student_properties, points_possible, points_scored, question_text in values:
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
                if question_text:
                    latest_question_text = question_text
                latest_tags = saved_tags.copy() if saved_tags else None
                latest_points_possible = points_possible

            total_earned_points_info.append(points_scored)
            num_submissions_count += 1

            for prop_type, prop_dict in student_properties.iteritems():
                if prop_dict:
                    if prop_dict not in props:
                        props.append(prop_dict)
                        props_info.append({
                            'type': prop_type,
                            'total_earned_points_info': [],
                            'num_submissions_count': 0,
                        })
                    prop_idx = props.index(prop_dict)
                    props_info[prop_idx]['total_earned_points_info'].append(points_scored)
                    props_info[prop_idx]['num_submissions_count'] += 1

        # convert properties dict to the JSON format

        props_list_values = []
        if len(props) > 0:
            for i, prop_dict in enumerate(props):
                props_list_values.append({
                    'props': prop_dict,
                    'type': props_info[i]['type'],
                    'total_earned_points': self._sum_earned_points(props_info[i]['total_earned_points_info']),
                    'total_earned_points_dist': self._dist_earned_points_info(props_info[i]['total_earned_points_info']),
                    'num_submissions_count': props_info[i]['num_submissions_count'],
                    'points_possible': latest_points_possible
                })
            props_json = json.dumps(props_list_values)

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

        name_hash = hashlib.md5(criterion_name).hexdigest()

        # save values to the database table

        yield StudentPropertiesAndOraTagsRecord(
            course_id=course_id,
            org_id=org_id,
            course=course,
            run=run,
            module_id=ora_id,
            criterion_name=criterion_name,
            question_text=latest_question_text,
            name_hash=name_hash,
            assessment_type=assessment_type,
            properties_data=props_json,
            tag_name=None,
            tag_value=None,
            possible_points=latest_points_possible,
            total_earned_points=self._sum_earned_points(total_earned_points_info),
            total_earned_points_dist=json.dumps(self._dist_earned_points_info(total_earned_points_info)),
            submissions_count=num_submissions_count).to_string_tuple()

        if latest_tags:
            for tag_key, tags_extended_lst in tags_extended_dict.iteritems():
                for val in tags_extended_lst:
                    yield StudentPropertiesAndOraTagsRecord(
                        course_id=course_id,
                        org_id=org_id,
                        course=course,
                        run=run,
                        module_id=ora_id,
                        criterion_name=criterion_name,
                        question_text=latest_question_text,
                        name_hash=name_hash,
                        assessment_type=assessment_type,
                        properties_data=props_json,
                        tag_name=tag_key,
                        tag_value=val,
                        possible_points=latest_points_possible,
                        total_earned_points=self._sum_earned_points(total_earned_points_info),
                        total_earned_points_dist=json.dumps(self._dist_earned_points_info(total_earned_points_info)),
                        submissions_count=num_submissions_count).to_string_tuple()


class StudentPropertiesAndOraTagsRecord(Record):
    course_id = StringField(length=255, nullable=False, description='Course id')
    org_id = StringField(length=255, nullable=False, description='Org id')
    course = StringField(length=255, nullable=False, description='Course')
    run = StringField(length=255, nullable=False, description='Run')
    module_id = StringField(length=255, nullable=False, description='ORA id')
    criterion_name = StringField(length=255, nullable=False, description='Criterion name')
    question_text = StringField(length=21844, nullable=True, description='Question Text')
    name_hash = StringField(length=255, nullable=True, description='Name Hash')
    assessment_type = StringField(length=255, nullable=False, description='Assessment type')
    properties_data = StringField(length=21844, nullable=True, description='Properties data in JSON format')
    tag_name = StringField(length=255, nullable=True, description='Tag key')
    tag_value = StringField(length=255, nullable=True, description='Tag value')
    possible_points = IntegerField(nullable=False, description='Possible points')
    total_earned_points = IntegerField(nullable=False, description='Total earned points')
    submissions_count = IntegerField(nullable=False, description='Submissions count')
    total_earned_points_dist = StringField(length=21844, nullable=True, description='Distribution of earned points')


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
