import os
import requests
from urlparse import urljoin


class ArchiveJobResource:
    def __init__(self):
        mongo_service = os.environ['MONGOSERVICE'] if 'MONGOSERVICE' in os.environ else 'mongoservice'
        self.repository_name = os.environ['ARCHIVE_NAME'] if 'ARCHIVE_NAME' in os.environ else 'archive'
        self.mongo_base_url = 'http://' + mongo_service

    def list_archive_jobs(self):
        url = '/api/v1/' + self.repository_name + '/archive_job'
        try:
            response = requests.get(urljoin(self.mongo_base_url, url))
            return response.json()
        except Exception as e:
            if response.status_code == 200:
                return None
            else:
                print e.message
                return None

    def get_archive_job(self, index_name):
        url = '/api/v1/' + self.repository_name + '/archive_job'
        try:
            query = {'index': index_name}
            response = requests.get(urljoin(self.mongo_base_url, url), params=query)
            return response.json()
        except Exception as e:
            print e.message
            return None

    def create_archive_job(self, index_name, retention):
        url = '/api/v1/' + self.repository_name + '/archive_job'
        try:
            payload = {'index': str(index_name), 'retention': str(retention)}
            response = requests.post(urljoin(self.mongo_base_url, url), json=payload)
            return response.json()
        except Exception as e:
            print e.message
            return None

    def update_archive_job(self, index_name, retention):
        url = '/api/v1/' + self.repository_name + '/archive_job'
        try:
            payload = {'index': str(index_name), 'retention': str(retention)}
            query = {'index': index_name}
            response = requests.put(urljoin(self.mongo_base_url, url), params=query, json=payload)
            return response.json()
        except Exception as e:
            print e.message
            return None

    def delete_archive_job(self, index_name):
        url = '/api/v1/' + self.repository_name + '/archive_job'
        try:
            query = {'index': index_name}
            response = requests.delete(urljoin(self.mongo_base_url, url), json=query)
            if response.status_code == 204:
                return True
        except Exception as e:
            if response.status_code == 204:
                return True
            else:
                print e.message
                return False


def main():
    archive_job_resource = ArchiveJobResource()
    print archive_job_resource.list_archive_jobs()
    for archive_job in archive_job_resource.list_archive_jobs():
        archive_job_resource.delete_archive_job(archive_job['index'])
    print archive_job_resource.list_archive_jobs()
    """
    print archive_job_resource.get_archive_job('nginx2')
    print archive_job_resource.delete_archive_job('nginx')

    print archive_job_resource.list_archive_jobs()

    print archive_job_resource.create_archive_job('nginx', '7')
    print archive_job_resource.list_archive_jobs()

    print archive_job_resource.update_archive_job('nginx', '15')
    print archive_job_resource.list_archive_jobs()
    """
if __name__ == "__main__":
    main()

