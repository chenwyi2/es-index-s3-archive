import json

from flask import Flask, request
from archivejobresource import ArchiveJobResource
from snapshotresource import SnapshotResource

app = Flask(__name__)


@app.route('/api/v1/archive_job', methods=['GET'])
def list_archive_jobs():
    archive_job_resource = ArchiveJobResource()
    archive_jobs = archive_job_resource.list_archive_jobs()
    return json.dumps(archive_jobs), 200


@app.route('/api/v1/archive_job/<index_pattern>', methods=['GET'])
def get_archive_job(index_pattern):
    archive_job_resource = ArchiveJobResource()
    archive_job = archive_job_resource.get_archive_job(index_name=index_pattern)
    return json.dumps(archive_job), 200


@app.route('/api/v1/archive_job/<index_pattern>', methods=['PUT'])
def update_archive_job(index_pattern):
    archive_job_resource = ArchiveJobResource()
    request_payload = request.get_json()
    if request_payload and request_payload['index'] == index_pattern:
        r = archive_job_resource.update_archive_job(index_name=index_pattern, retention=request_payload['retention'])
        return r, 200
    else:
        return '', 204


@app.route('/api/v1/archive_job', methods=['POST'])
def create_archive_job():
    default_retention = '15'
    # TODO modify default_retention when going live
    snapshotresource = SnapshotResource()
    index_patterns = snapshotresource.list_index_patterns()
    archive_job_resource = ArchiveJobResource()
    for index_pattern in index_patterns:
        archive_job = archive_job_resource.get_archive_job(index_pattern)
        if not archive_job:
            archive_job_resource.create_archive_job(index_pattern, default_retention)
    return '', 204


@app.route('/api/v1/index_pattern', methods=['GET'])
def list_index_pattern():
    snapshotresource = SnapshotResource()
    response = snapshotresource.list_index_patterns()
    return json.dumps(response), 200


@app.route('/api/v1/index_pattern/<index_pattern>', methods=['GET'])
def list_indices(index_pattern):
    snapshotresource = SnapshotResource()
    response = snapshotresource.list_indices(index_pattern + '*')
    return json.dumps(response), 200


@app.route('/api/v1/snapshot', methods=['GET'])
def get_snapshot():
    snapshotresource = SnapshotResource()
    try:
        snapshotresource.create_s3_repository()
        snapshot = snapshotresource.get_s3_snapshot('*')
        return json.dumps(snapshot), 200
    except Exception as e:
        print (e.info)
        return e.info, e.status_code


@app.route('/api/v1/snapshot', methods=['POST'])
def create_snapshot():
    snapshotresource = SnapshotResource()
    try:
        snapshotresource.create_s3_repository()
        snapshot = snapshotresource.create_s3_snapshot()
        return json.dumps(snapshot), 200
    except Exception as e:
        print (e.info)
        return e.info, e.status_code


def main():
    app.run(host='0.0.0.0')

if __name__ == "__main__":
    main()
