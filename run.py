from pytz import utc
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from pymongo import MongoClient
import logging
from flask import Flask, request, current_app
from flask_restplus import Api, Resource, Namespace, fields
from flask_cors import CORS
from datetime import datetime
import configparser
import os

logger = logging.getLogger("APSDebugger")


def alarm():
    time = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    print('Alarm! This alarm was scheduled at %s.' % time)


class JobScheduler():
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), '../config/' + 'config.ini'))
        self._scheduler = BackgroundScheduler()

    def _config_scheduler(self):
        """Initialize and return a scheduler
        """
        import logging
        logging.basicConfig()
        logging.getLogger('apscheduler').setLevel(logging.DEBUG)
        client = MongoClient(self.config['piper']['mongodb_uri'], connect=False)
        jobstores = {
            'mongodb': MongoDBJobStore(database=self.config['piper']['piperdb'],
                                       collection=self.config['piper']['apscheduler'],
                                       client=client)
        }
        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        job_defaults = {'coalesce': False, 'max_instances': 1}
        self._scheduler.configure(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)

    def init_app(self, app):
        self.app = app
        self.app.apscheduler = self
        self._config_scheduler()

    def add_job(self, schedule_id):
        self._scheduler.add_job(func=alarm,id=schedule_id,replace_existing=True,jobstore='mongodb',trigger='interval',seconds=10)
        logger.info("Job scheduled ===> scheduler state: | {}".format(self.state))

    def remove_job(self, schedule_id):
        self._scheduler.remove_job(schedule_id)

    def get_job(self, schedule_id):
        logger.info("Job scheduler state is: | {}".format(self.state))
        # if self.state != base_scheduler.STATE_RUNNING:
        #     self.start()
        job = self._scheduler.get_job(schedule_id, jobstore='mongodb')
        return job
    @property
    def state(self):
        """Get the state of the scheduler.
        STATE_STOPPED = 0: constant indicating a scheduler's stopped state
        STATE_RUNNING = 1: constant indicating a scheduler's running state (started and processing jobs)
        STATE_PAUSED = 2: constant indicating a scheduler's paused state (started but not processing jobs)
        """
        return self._scheduler.state
    def start(self, paused=False):
        self._scheduler.start(paused=paused)

class JobScheduleDto:
    api = Namespace('Job Schedule', description='(Add/remove schedule)')
    add_job_schedule = api.model('add_job_schedule', {
        'example_mandatory_arg1': fields.String(required=False, description='Ex. mandatory argument'),
        'example_optional_arg1': fields.String(required=False, description='Ex. optional argument')})
    get_job_schedule = api.model('get_job_schedule', {
        'schedule_id': fields.String(required=False, description='Job schedule id')
    })
api = JobScheduleDto.api
@api.route('/get_scheduler_state')
class JobSchedule(Resource):
    @api.doc('Get scheduler state')
    def get(self):
        logger.info("REsT API received a request : {}. request body contains : {}".format(request, request.json))
        scheduler_state = "RUNNING" if current_app.apscheduler.state == 1 else "NOT RUNNING"
        return {"Scheduler state": scheduler_state}
@api.route('/<string:schedule_id>')
@api.param('schedule_id', 'Schedule ID')
class JobScheduleRoute(Resource):
    @api.doc('Get a job schedule info')
    def get(self, schedule_id):
        logger.info("REsT API received a request : {}. request body contains : {}".format(request, request.json))
        job_schedule = current_app.apscheduler.get_job(schedule_id)
        return job_schedule

_job_schedule_api = api
def create_app():
    logger.info("Initiating Flask-REsT API.")
    app = Flask(__name__)
    api = Api(app,title='APS',version='1.0',)
    api.namespaces = []
    CORS(app)
    api.add_namespace(_job_schedule_api, path='/schedule')
    return app

if __name__ == '__main__':
    app = create_app()
    scheduler = JobScheduler()
    scheduler.init_app(app)
    scheduler.start()
    # scheduler.add_job("test_job1")
    app.run(debug=False, host='0.0.0.0', port=5555)
