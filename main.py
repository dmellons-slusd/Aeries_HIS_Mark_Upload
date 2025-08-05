from pandas import read_csv, read_sql_query
from slusdlib import aeries, core, decorators
from sqlalchemy import text
import re

passing_marks = ["A+","A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-"]
def next_sq( table: str, cnxn = aeries.get_aeries_cnxn(), id: str = None, sn: str = None, sc: str = None) -> int:
    if not table:
        raise ValueError("Table name is required.")
    if id:
        sql = text(f"select top 1 sq from {table} where PID = :id order by sq desc")
        params = {"id": id}
    else:
        sql = text(f"select top 1 sq from {table} where SN = :sn and SC = :sc order by sq desc")
        params = {"sn": sn, "sc": sc}
    with cnxn.connect() as conn:
        result = conn.execute(sql, params)
        row = result.fetchone()
        if row:
            return row[0] + 1
        else:
            return 1
        
def get_id_from_email(email:str) -> str:
    return re.search(r'(\d+)@', email).group(1)

def get_course_details(course_code: str, cnxn = aeries.get_aeries_cnxn()) -> dict:
    sql = text("SELECT * FROM CRS WHERE CN = :course_code")
    data = read_sql_query(sql, cnxn, params={"course_code": course_code})
    keep_columns = ['CN', 'CO', 'CC', 'CR']
    data = data[keep_columns]
    return data.to_dict(orient='records')[0] if not data.empty else None

def get_student_previous_grade_level(id: str, cnxn = aeries.get_aeries_cnxn()) -> int:
    sql = text("SELECT gr FROM DST24000SLUSD..STU WHERE ID = :id and tg = '' and del = 0")
    data = read_sql_query(sql, cnxn, params={"id": id})
    return data['gr'].iloc[0] if not data.empty else None

def insert_new_his_record(cnxn, id: str, sq: int, cn: str, mk: str, cr: int, co: str, gr:int, te:int = 3, yr:int = 24, st: int = 916):
    if mk in passing_marks:
        cc = cr
    else:
        cc = 0
    if gr < 9:
        core.log(f"Invalid grade level for student ID {id}. Skipping record.")
        raise ValueError(f"Invalid grade level for student ID {id}. Skipping record.")
    sql = text("""
    INSERT INTO HIS (
        PID, 
        CN, 
        MK, 
        CR, 
        CO,
        GR, 
        TE, 
        YR, 
        ST, 
        CC, 
        SQ, 
        SID,
        SDE, 
        CH, 
        DEL
    ) VALUES (
        :pid, 
        :cn, 
        :mk, 
        :cr,
        :co, 
        :gr, 
        :te, 
        :yr, 
        :st, 
        :cc, 
        :sq,
        :sid, 
        :sde, 
        :ch, 
        0
    );
    """)
    params = {
        "pid": str(id),
        "cn": str(cn),
        "mk": str(mk),
        "cr": float(cr),
        "co": str(co),
        "gr": int(gr),  
        "te": int(te),
        "yr": int(yr),
        "st": int(st),
        "cc": float(cc),
        "sq": int(sq),
        "sid": str(""),
        "sde": str(""),
        "ch": str("")
    }
    try:
        with cnxn.connect() as conn:
            conn.execute(sql, params)
            conn.commit()
    except Exception as e:
        core.log(e)
        raise ValueError(f"Failed to insert new HIS record for ID {id}: {e}")
    
@decorators.log_function_timer
def main():
    file = 'in_data/25-26 SLUSD Subject Summer School Completion _ Non-completion Data - Completions.csv'
    data = read_csv(file)
    cnxn = aeries.get_aeries_cnxn( access_level='w')
    for index, row in data.iterrows():
        pid = get_id_from_email(row['Email'])
        mk = row['GRADE TO ADD SUMMER AERIES']
        if mk not in passing_marks:
            core.log(f"ID {pid} has invalid grade mark: {mk}. Skipping record ")
            core.log('~' * 70)
            continue
        sq = next_sq(table="HIS", cnxn=cnxn, id=pid)
        cn = row['Aeries CN']

        term = 3
        course = get_course_details(cn, cnxn=cnxn)
        cr = course['CR']
        co = course['CO']

        if not course:
            core.log(f"Course not found: {row['Course']}")
            raise ValueError(f"Course not found: {row['Course']}")
        grade = get_student_previous_grade_level(pid, cnxn=cnxn)

        if grade < 0 or grade is None:
            raise ValueError(f"Invalid grade level for student ID {pid}. Skipping record.")

        
        insert_new_his_record(cnxn, id=pid, sq=sq, cn=cn, mk=mk, cr=int(cr), co=co, gr=grade)
        core.log(f"Inserted new HIS record for ID# {pid} with SQ:: {sq}, CN: {cn}, MK: {mk}, CR: {cr}, CO: {co}, GR: {grade}")
        core.log('~' * 70)

if __name__ == "__main__":
    core.log('#' * 80)
    main()
    core.log('#' * 80)