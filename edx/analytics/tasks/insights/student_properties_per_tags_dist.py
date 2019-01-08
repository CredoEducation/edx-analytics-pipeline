import logging
import luigi
import hashlib
import json
import re
import copy

import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.util.url import get_target_from_url
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util.record import Record, StringField, IntegerField, FloatField


log = logging.getLogger(__name__)


def get_value_from_student_properties(key, properties):
    types = ['registration', 'enrollment']
    key_updated = key.strip().lower()
    new_value = None
    new_properties = properties.copy()

    for tp in types:
        if tp in new_properties:
            tmp_properties = {}
            for k in new_properties[tp]:
                tmp_properties[k.strip().lower()] = k
            for tk, tv in tmp_properties.iteritems():
                if tk == key_updated:
                    new_value = new_properties[tp][tv].replace('+', '-').replace(' ', '_')\
                        .replace("\n", "").replace("\t", "").replace("\r", "")
                    del new_properties[tp][tv]
    return new_value, new_properties


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

    def _get_question_text(self, event_data):
        question_text = ''
        submissions = event_data.get('submission', {})
        if submissions:
            for _, submission in submissions.iteritems():
                if submission:
                    q_text = submission.get('question', '')
                    if q_text:
                        question_text = q_text
        return question_text

    def _get_answer_values(self, event_data):
        answers = event_data['answers']
        submissions = event_data.get('submission', {})
        correct_maps = event_data.get('correct_map', {})
        result_answers = []
        for answer_id, submission in submissions.items():
            if submission['input_type'] and submission['input_type'] in ['choicegroup', 'checkboxgroup']:
                answer_data = {}
                answer_value = answers[answer_id]
                answer_data['answer_value'] = '|'.join(answer_value if isinstance(answer_value, list) else [answer_value])

                answers_text = submission['answer'] if isinstance(submission['answer'], list) else [submission['answer']]
                processed_answers = []
                for item in answers_text:
                    processed_answers.append(re.sub('<choicehint\s*(selected=\"true\")*>.*?</choicehint>', '',
                                                    item.replace("\n", "").replace("\t", "").replace("\r", "")))
                answer_data['answer_display'] = '|'.join(processed_answers)
                answer_data['correct'] = int(submission.get('correct', -1))
                answer_data['correctness'] = correct_maps[answer_id].get('correctness', '')

                result_answers.append(answer_data)
        return result_answers

    def _get_empty_rubric_answers(self, event_data):
        answers_parts = event_data.get('answer', {}).get('parts')
        if answers_parts is None:
            return ''

        answer = ' '.join([i.get('text', '') for i in answers_parts])
        return [{'answer_value': answer, 'correct': 1, 'correctness': 'correct'}]

    def _get_dnd_correctness(self, event_data):
        correct = 0
        correctness = 'incorrect'
        earned_grade = float(event_data.get('earned_score', 0))
        max_grade = float(event_data.get('max_score', 0))

        if 0 < earned_grade < max_grade:
            correct = 1
            correctness = 'partially-correct'
        elif earned_grade == max_grade:
            correct = 1
            correctness = 'correct'
        return {
            'correct': correct,
            'correctness': correctness,
            'earned_grade': earned_grade,
            'max_grade': max_grade
        }

    def _get_dnd_answer_values(self, event_data):
        items_state = {}
        for st in event_data.get('item_state'):
            zone_title = st.get('zone', {}).get('title', '')
            if zone_title:
                items_state[zone_title] = {
                    'display_name': st.get('display_name', ''),
                    'id': str(st.get('id', '0'))
                }

        answer_display_list = []
        answer_value_list = []
        zones = [z['title'] for z in event_data.get('zones', []) if 'title' in z]
        for zone_title in zones:
            item_state = items_state.get(zone_title)
            answer_display_list.append(item_state['display_name'] if item_state else '-Empty-')
            answer_value_list.append(item_state['id'] if item_state else '-1')

        dnd_correctness = self._get_dnd_correctness(event_data)

        answer_data = {
            'answer_value': '|'.join(answer_value_list),
            'answer_display': '|'.join(answer_display_list),
            'correct': dnd_correctness['correct'],
            'correctness': dnd_correctness['correctness']
        }
        return [answer_data]

    def _count_answer_values(self, user_answers):
        result = {}
        for user_id, user_answers in user_answers.iteritems():
            for item in user_answers:
                answer_value = item['answer_value']
                result.setdefault(answer_value, copy.copy(item))
                result[answer_value]['count'] = result[answer_value].get('count', 0) + 1
                if 'users' not in result[answer_value]:
                    result[answer_value]['users'] = []
                result[answer_value]['users'].append(user_id)
                result[answer_value]['correct'] = item['correct']
                result[answer_value]['correctness'] = item['correctness']

        return result

    def check_is_ora_empty_rubrics(self, event_type, event_data):
        if event_type == 'openassessmentblock.create_submission':
            if event_data.get('rubric_count') == 0:
                return True
        return False

    def mapper(self, line):
        """
        Args:
            line: text line from a tracking event log.

        Yields:  (course_id, org_id, course, run, problem_id), (timestamp, saved_tags, student_properties, is_correct,
                                                                grade, user_id, display_name, question_text)

        """
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _ = value

        event_type = event.get('event_type')

        if event_type not in ('problem_check', 'edx.drag_and_drop_v2.item.dropped',
                              'openassessmentblock.create_submission')\
                or event.get('event_source') != 'server':
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        is_dnd_problem = event_type == 'edx.drag_and_drop_v2.item.dropped'

        is_ora_empty_rubrics = self.heck_is_ora_empty_rubrics(event_type, event_data)

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        user_id = event.get('context').get('user_id', None)

        if not user_id:
            return

        org_id = opaque_key_util.get_org_id_for_course(course_id)
        course = opaque_key_util.get_course_for_course(course_id)
        run = opaque_key_util.get_run_for_course(course_id)

        if is_dnd_problem and 'zones' not in event_data:
            return

        if is_dnd_problem:
            problem_id = event.get('context').get('module', {}).get('usage_key')
        elif is_ora_empty_rubrics:
            problem_id = event.get('context', {}).get('module', {}).get('usage_key')
        else:
            problem_id = event_data.get('problem_id')

        if not problem_id:
            return

        if is_dnd_problem:
            dnd_correctness = self._get_dnd_correctness(event_data)
            is_correct = dnd_correctness['correct'] == 1
            earned_grade = dnd_correctness['earned_grade']
            max_grade = dnd_correctness['max_grade']
        elif is_ora_empty_rubrics:
            is_correct = True
            max_grade = 0
        else:
            is_correct = event_data.get('success') == 'correct'
            earned_grade = float(event_data.get('grade', 0))
            max_grade = float(event_data.get('max_grade', 0))

        if max_grade != 0:
            grade = earned_grade / max_grade
        else:
            grade = 1 if is_correct else 0

        display_name = event.get('context').get('module', {}).get('display_name', '')
        question_text = self._get_question_text(event_data)
        question_text = question_text.replace("\n", " ").replace("\t", " ").replace("\r", "")

        saved_tags = event.get('context').get('asides', {}).get('tagging_aside', {}).get('saved_tags', {})
        student_properties = event.get('context').get('asides', {}).get('student_properties_aside', {})\
            .get('student_properties', {})

        overload_items = {
            'course': {'value': course, 'props': ['course', 'courses', 'course_title', 'course title', 'othercourse']},
            'term': {'value': None, 'props': ['term', 'terms', 'run', 'runs']}
        }
        for k in overload_items:
            for prop in overload_items[k]['props']:
                new_value, new_properties = get_value_from_student_properties(prop, student_properties)
                if new_value:
                    overload_items[k]['value'], student_properties = new_value, new_properties

        if overload_items['term']['value']:
            student_properties['enrollment']['terms'] = overload_items['term']['value']

        if is_dnd_problem:
            answers = self._get_dnd_answer_values(event_data)
        elif is_ora_empty_rubrics:
            answers = self._get_empty_rubric_answers(event_data)
        else:
            answers = self._get_answer_values(event_data)

        yield (course_id, org_id, overload_items['course']['value'], run, problem_id),\
              (timestamp, saved_tags, student_properties, is_correct, grade, int(user_id), display_name, question_text,
               answers)

    def reducer(self, key, values):
        """
        Calculate the count of total/correct submissions for each pair:
        problem + related tag + related students properties

        Args:
            key:  (course_id, org_id, course, run, problem_id)
            values:  iterator of (timestamp, saved_tags, student_properties, is_correct,
                                  grade, user_id, display_name, question_text)

        """
        course_id, org_id, course, run, problem_id = key

        num_total = 0
        num_correct = 0
        num_correct_grade = 0
        all_answers_json = None

        user2total = {}
        user2correct = {}
        user2correct_grade = {}
        user2last_timestamp = {}
        user2answers = {}

        latest_timestamp = None
        latest_display_name = u''
        latest_question_text = u''
        latest_tags = None

        props = []
        props_info = []
        props_json = None
        all_users_data = {}
        all_users_data_json = None

        # prepare base dicts for tags and properties

        for timestamp, saved_tags, student_properties, is_correct,\
                grade, user_id, display_name, question_text, answers in values:

            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
                if display_name:
                    latest_display_name = display_name
                if question_text:
                    latest_question_text = question_text
                latest_tags = saved_tags.copy() if saved_tags else None

            if user_id not in all_users_data:
                all_users_data[user_id] = [0, 0]

            current_user_last_timestamp = user2last_timestamp.get(user_id, None)
            if current_user_last_timestamp is None or timestamp > current_user_last_timestamp:
                user2last_timestamp[user_id] = timestamp
                user2correct[user_id] = 1 if is_correct else 0
                user2correct_grade[user_id] = grade
                user2answers[user_id] = answers
                all_users_data[user_id] = [1 if is_correct else 0, grade]

            user2total[user_id] = 1

            for prop_type, prop_dict in student_properties.iteritems():
                if prop_dict:
                    if prop_dict not in props:
                        props.append(prop_dict)
                        props_info.append({
                            'type': prop_type,
                            'num_total': {},
                            'num_correct': {},
                            'num_correct_grade': {},
                            'answers': {},
                            'user_last_timestamp': {},
                        })
                    prop_idx = props.index(prop_dict)
                    prop_current_user_last_timestamp = props_info[prop_idx]['user_last_timestamp'].get(user_id, None)
                    if prop_current_user_last_timestamp is None or timestamp > prop_current_user_last_timestamp:
                        props_info[prop_idx]['user_last_timestamp'][user_id] = timestamp
                        props_info[prop_idx]['num_correct'][user_id] = 1 if is_correct else 0
                        props_info[prop_idx]['num_correct_grade'][user_id] = grade
                        props_info[prop_idx]['answers'][user_id] = answers
                    props_info[prop_idx]['num_total'][user_id] = 1

        if user2total:
            num_total = sum(user2total.values())

        if user2correct:
            num_correct = sum(user2correct.values())

        if user2correct_grade:
            num_correct_grade = sum(user2correct_grade.values())

        if user2answers:
            all_answers = self._count_answer_values(user2answers)
            all_answers_json = json.dumps(all_answers)

        if all_users_data:
            all_users_data_json = json.dumps(all_users_data)

        # convert properties dict to the JSON format

        props_list_values = []
        if len(props) > 0:
            for i, prop_dict in enumerate(props):
                u_data = {}
                for u_id, u_val in props_info[i]['num_correct'].iteritems():
                    u_data[u_id] = [u_val, props_info[i]['num_correct_grade'][u_id]]

                props_list_values.append({
                    'props': prop_dict,
                    'type': props_info[i]['type'],
                    'total': sum(props_info[i]['num_total'].values()),
                    'correct': sum(props_info[i]['num_correct'].values()),
                    'correct_grade': sum(props_info[i]['num_correct_grade'].values()),
                    'answers': self._count_answer_values(props_info[i]['answers']),
                    'users': u_data
                })
            props_json = json.dumps(props_list_values)

        # convert latest tags dict to extended list. Example:
        # { 'lo': ['AAC&U VALUE Rubric - Written Communication - Genre and Disciplinary Conventions',
        #          'Paul & Elder Critical Thinking Model - Concepts and Ideas'] }
        # =>
        # ['AAC&U VALUE Rubric', 'AAC&U VALUE Rubric - Written Communication',
        #  'AAC&U VALUE Rubric - Written Communication - Genre and Disciplinary Conventions',
        #  'Paul & Elder Critical Thinking Model'
        #  'Paul & Elder Critical Thinking Model - Concepts and Ideas']

        tags_extended_lst = []
        tags_extended_lst_json = None
        if latest_tags:
            for tag_key, tag_val in latest_tags.iteritems():
                tag_val_lst = [tag_val] if isinstance(tag_val, basestring) else tag_val
                for tag in tag_val_lst:
                    tag_split_lst = tag.split(' - ')
                    for idx, tag_part in enumerate(tag_split_lst):
                        tag_new_val = ' - '.join(tag_split_lst[0:idx + 1])
                        if tag_new_val not in tags_extended_lst:
                            tags_extended_lst.append(tag_new_val)

        if tags_extended_lst:
            tags_extended_lst_json = json.dumps(tags_extended_lst)

        common_name = u''.join([latest_display_name, latest_question_text])
        name_hash = hashlib.md5(common_name.encode('utf-8')).hexdigest()

        # save values to the database table

        yield StudentPropertiesAndTagsRecord(
            course_id=course_id,
            org_id=org_id,
            course=course,
            run=run,
            module_id=problem_id,
            display_name=latest_display_name,
            question_text=latest_question_text,
            name_hash=name_hash,
            properties_data=props_json,
            tags=tags_extended_lst_json,
            total_submissions=num_total,
            correct_submissions=num_correct,
            correct_submissions_grades=num_correct_grade,
            answers=all_answers_json,
            users=all_users_data_json).to_string_tuple()


class StudentPropertiesAndTagsRecord(Record):
    course_id = StringField(length=255, nullable=False, description='Course id')
    org_id = StringField(length=255, nullable=False, description='Org id')
    course = StringField(length=255, nullable=False, description='Course')
    run = StringField(length=255, nullable=False, description='Run')
    module_id = StringField(length=255, nullable=False, description='Problem id')
    display_name = StringField(length=2048, nullable=True, description='Problem Display Name')
    question_text = StringField(length=500000, nullable=True, description='Question Text')
    name_hash = StringField(length=255, nullable=True, description='Name Hash')
    properties_data = StringField(length=500000, nullable=True, description='Properties data in JSON format')
    tags = StringField(length=500000, nullable=True, description='Tags')
    total_submissions = IntegerField(nullable=False, description='Number of total submissions')
    correct_submissions = IntegerField(nullable=False, description='Number of correct submissions')
    correct_submissions_grades = FloatField(nullable=False, description='Number of correct submissions include partial correctness')
    answers = StringField(length=500000, nullable=True, description='Distribution of answers')
    users = StringField(length=500000, nullable=True, description='Distribution of users')


@workflow_entry_point
class StudentPropertiesAndTagsDistributionWorkflow(StudentPropertiesPerTagsPerCourseDownstreamMixin,
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
            ('module_id',),
        ]

