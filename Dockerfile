FROM 10.94.34.96/middleware/python-uwsgi:1.16
COPY * /snapshot_service/
WORKDIR /snapshot_service
RUN pip install --proxy="http://saicmotor:Pass_2019@10.90.3.172:22222" -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com -r /snapshot_service/requirements.txt
CMD ["uwsgi", "--uid", "uwsgi", "--plugin", "python", "--ini", "/snapshot_service/snapshotService.ini"]