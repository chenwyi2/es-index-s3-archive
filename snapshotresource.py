import datetime
import os
import re
import time

import elasticsearch

from archivejobresource import ArchiveJobResource
from bucketresource import BucketResource


class SnapshotResource:
    def __init__(self):
        # env ES_HOSTS: one host
        # TODO: array ES_HOSTS input can not be recognized
        es_hosts = os.environ['ES_HOSTS'] if 'ES_HOSTS' in os.environ else ''
        access_key = os.environ['S3AK'] if 'S3AK' in os.environ else ''
        secret_key = os.environ['S3SK'] if 'S3SK' in os.environ else ''
        self.s3_host = os.environ['S3HOST'] if 'S3HOST' in os.environ else ''
        self.s3resource = BucketResource(access_key, secret_key, 'http://' + self.s3_host)
        self.archive_job_resource = ArchiveJobResource()
        self.collection = elasticsearch.Elasticsearch(hosts=es_hosts,
                                                      connection_class=elasticsearch.RequestsHttpConnection)
        self.repository_name = os.environ['ARCHIVE_NAME'] if 'ARCHIVE_NAME' in os.environ else 'archive'

    def list_indices(self, pattern):
        """
        List all indices of the pattern
        :param pattern: string, same with pattern in _cat/indices like '*' or 'nginx-*'
        :return: list, index names, [] for none
        """
        indices = dict(self.collection.indices.get(pattern)).keys()
        return indices

    def list_index_patterns(self):
        """
        List all index patterns, by matching regex '^(\S+)-20'
        Only get the index patterns like 'nginx-kafka' or 'kong', ignore index pattern without timestamp postfix
        :return: list, index patterns, [] for none
        """
        indices = self.list_indices('*')
        index_patterns = set()
        for index in indices:
            match_obj = re.match('^(\S+)-20', index)
            if match_obj:
                index_patterns.add(match_obj.group(1))
        return list(index_patterns)

    def get_s3_repository(self):
        """
        Get snapshots in the repository(self.repository_name)
        :return: list, names of the snapshots, [] for none
        """
        response = self.collection.snapshot.get_repository(repository=self.repository_name).keys()
        return response

    def create_s3_repository(self):
        """
        Create a repository named by self.repository_name
        OSS bucket is generated with name 'repository_name-YYYYWW'
        :return: Boolean, True for create request acknowledged, False for other situation
        """
        # elasticsearch-keystore has already been added on every elasticsesarch node and reloaded
        bucket_name = self.repository_name + '-' + time.strftime("%Y%W")
        if not self.s3resource.get_bucket(bucket_name):
            self.s3resource.create_bucket(bucket_name)
        data = {
            "type": "s3",
            "settings":
                {
                    "bucket": bucket_name,
                    "endpoint": self.s3_host,
                    "protocol": "http"
                }
        }
        response = self.collection.snapshot.create_repository(repository=self.repository_name, body=data)
        if 'acknowledged' in response:
            if response['acknowledged']:
                return True
        return False

    def get_s3_snapshot(self, snapshot):
        """
        Get the snapshot by name in the repository of (self.repository_name)
        :param snapshot: snapshot name to get
        :return: list, snapshots with the name passed in, [] for none
        """
        response = self.collection.snapshot.get(repository=self.repository_name,
                                                snapshot=snapshot,
                                                ignore_unavailable=True)['snapshots']
        return response

    def create_s3_snapshot(self):
        """
        Create a snapshot named by 'snapshot-YYYYMMDD',
        Get all indices with all index patterns -> flush indices -> create snapshot -> detect snapshot creation result
        in while True loop -> if SUCCESS delete indices
        Only return true if creation succeed
        :return: dic:{string, snapshot_name; list, names of indices which are archived;
                string, msg, create_snapshot api response}
                None for error or none
        """
        snapshot_name = 'snapshot-' + datetime.date.today().strftime('%Y%m%d')
        indices = []
        if not self.get_s3_snapshot(snapshot=snapshot_name):
            for index_pattern in self.list_index_patterns():
                indices.extend(self.get_archive_index_name(index_pattern,
                                                           self.get_archive_index_retention(index_pattern)
                                                           )
                               )
        try:
            sep = ','
            index = sep.join(indices)
            flush_response = self.collection.indices.flush(index=index, ignore_unavailable=True, wait_if_ongoing=True)
            data = {
                "indices": index,
                "ignore_unavailable": True,
                "include_global_state": False
            }
            create_response = self.collection.snapshot.create(repository=self.repository_name,
                                                              snapshot=snapshot_name,
                                                              body=data)
            return {'snapshot_name': snapshot_name, 'indices': indices, 'msg': create_response}
        except Exception as e:
            print (e)
            return None

    def get_snapshot_status(self, snapshot_name, indices):
        try:
            sep = ','
            index = sep.join(indices)
            snapshot = self.get_s3_snapshot(snapshot=snapshot_name)
            while True:
                time.sleep(3)
                snapshot = self.get_s3_snapshot(snapshot=snapshot_name)
                if snapshot[0]['state'] != 'IN_PROGRESS':
                    break
            if snapshot[0]['state'] == 'SUCCESS':
                delete_response = self.collection.indices.delete(index=index, ignore_unavailable=True)
                return indices
            else:
                return None
        except Exception as e:
            print (e)
            return None

    def get_archive_index_name(self, index_pattern, retention):
        """
        Calculate archived index names for create_s3_snapshot
        :param index_pattern:
        :param retention:
        :return: list, index names for archive, [] for none
        """
        index = []
        try:
            raw_index_list = self.list_indices(index_pattern + '*')

            retention_date = datetime.date.today() - datetime.timedelta(days=retention)

            re_pattern = re.compile(index_pattern + r"-(\d{4}\.\d{2}\.\d{2})$")

            for i in raw_index_list:
                match_obj = re.match(re_pattern, i)
                if match_obj:
                    index_date = datetime.datetime.strptime(match_obj.group(1), '%Y.%m.%d').date()
                    if index_date.__le__(retention_date):
                        index.append(i)
            return index
        except Exception as e:
            print (e)
            return index

    def get_archive_index_retention(self, index_pattern):
        """
            Get retention for index_pattern,
            If not set, set the default retention by 7 days
            :param index_pattern:
            :return: int, retention days
        """
        # TODO modify default retention for testing
        default_retention = '15'
        archive_job = self.archive_job_resource.get_archive_job(index_pattern)
        if not archive_job:
            archive_job = self.archive_job_resource.create_archive_job(index_pattern, default_retention)
        else:
            archive_job = archive_job[0]
        return int(archive_job['retention'])


def main():
    snapshot_resource = SnapshotResource()
    print (snapshot_resource.list_index_patterns())
    print (snapshot_resource.create_s3_repository())
    print (snapshot_resource.create_s3_snapshot())


if __name__ == "__main__":
    main()
