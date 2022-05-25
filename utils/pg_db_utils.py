import sqlalchemy

def insertJob(engine, d):
    with engine.connect() as con:
        try:
            sql = '''
            INSERT INTO public.jobs("status", "type", "details")
            VALUES(:status, :type, :details)
            RETURNING id
            '''
            statement = sqlalchemy.sql.text(sql)
            row = con.execute(statement, **d)
            return {'ok': True, 'row': row.fetchone()}
        except Exception as e:
            print('insertJob error: ', e)
            return {'ok': False, 'error': e}

def updateJobStatus(engine, d):
    with engine.connect() as con:
        try:
            sql = '''
            UPDATE public.jobs
            SET "status" = :status, 
            "updated_at" = now()
            WHERE id = :id
            RETURNING id
            '''
            statement = sqlalchemy.sql.text(sql)
            row = con.execute(statement, **d)
            return {'ok': True, 'row': row.fetchone()}
        except Exception as e:
            print('updateJob error: ', e)
            return {'ok': False, 'error': e}

def updateJob(engine, d):
    with engine.connect() as con:
        try:
            sql = '''
            UPDATE public.jobs
            SET "status" = :status, 
            "details" = :details,
            "updated_at" = now()
            WHERE id = :id
            RETURNING id
            '''
            statement = sqlalchemy.sql.text(sql)
            row = con.execute(statement, **d)
            return {'ok': True, 'row': row.fetchone()}
        except Exception as e:
            print('updateJob error: ', e)
            return {'ok': False, 'error': e}

def getJobSummary(engine, t):
    with engine.connect() as con:
        sql = f'''
        select 
        max(id) as max_id, 
        min(id) as min_id, 
        count(id) as count,
        sum(case when j.status = 'running' then 1 else 0 end) as running
        from "public".jobs as j
        where j."type" = '{t}'
        '''
        statement = sqlalchemy.sql.text(sql)
        res = con.execute(statement).fetchone()
        return res

def getPendingJobs(engine):
    with engine.connect() as con:
        sql = f'''
        select 
        j."type", 
        min((j.details ->> 'maxRunning')::int) as max_running,
        sum(case when j."status" = 'running' then 1 else 0 end) as running,
        sum(case when j."status" = 'pending' then 1 else 0 end) as pending,
        count(1) as total
        from jobs as j
        -- where j."status" = 'pending'
        where true
        -- and j."type" = 'testJob'
        group by 1
        '''
        statement = sqlalchemy.sql.text(sql)
        rows = con.execute(statement)
        rows = rows.mappings().all()
        jobs = []
        for row in rows:
            capacity = 10
            # TODO: make order configurable from details
            order = row.get('order', 'asc')
            if row['max_running']:
                capacity = row['max_running'] - row['running']
            if capacity > 0:
                sql = f'''
                select 
                j.id,
                j.details
                from jobs as j
                where j."type" = '{row['type']}'
                and j."status" = 'pending'
                order by id {order}
                limit {capacity}
                '''
                statement = sqlalchemy.sql.text(sql)
                temp_jobs = con.execute(statement)
                temp_jobs = temp_jobs.mappings().all()
                _ = [jobs.append(dict(j)) for j in temp_jobs]
        print(f"getPendingJobs, num of jobs: {len(jobs)}")
        if len(jobs) > 0:
            print(f"getPendingJobs, first job: {jobs[0]}")
        return jobs

def getDoneMaxJob(engine, jobId):
    with engine.connect() as con:
        sql = f'''
        select *
        from "public".jobs as j
        where j.id = {jobId}
        and status = 'success'
        '''
        statement = sqlalchemy.sql.text(sql)
        res = con.execute(statement).fetchone()
        return res

def getMaxJob(engine, jobId):
    with engine.connect() as con:
        sql = f'''
        select *
        from "public".jobs as j
        where j.id = {jobId}
        '''
        statement = sqlalchemy.sql.text(sql)
        res = con.execute(statement).fetchone()
        return res