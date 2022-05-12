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