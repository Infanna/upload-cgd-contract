import pandas as pd 
from google.cloud import bigquery
import os
import time
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'C:/credential/ML/credential.json'
client = bigquery.Client()

lists = [1,2,3,4,5,6,7,8,9,10,11,12]

for i in lists:
    table_name = "cgd-contract"
    year = 2564
    number = i
    csv_name = f"./{year}/{year}-{table_name}_{number}.csv"
    df = pd.read_csv(csv_name, index_col=False)
    df = df.rename(columns=
    {
        'รหัสโครงการ': 'project_id',
        'ชื่อโครงการฯ': 'project_name',
        'ชื่อประเภทโครงการ': 'project_type_name',
        'ชื่อหน่วยงาน': 'dept_name',
        'ชื่อหน่วยงานย่อย': 'dept_sub_name',
        'ชื่อวิธีการฯ': 'purchase_method_name',
        'ชื่อกลุ่มวิธีการฯ': 'purchase_method_group_name',
        'วันที่ประกาศฯ': 'announce_date',
        'วงเงินงบประมาณ_บาท': 'project_money',
        'ราคากลาง_บาท': 'price_build',
        'ราคารวมทุกสัญญา_บาท': 'sum_price_agree',
        'ปีงบประมาณ': 'budget_year',
        'วันที่เกิดรายการ': 'transaction_date',
        'จังหวัด': 'province_thai',
        'จังหวัด_อังกฤษ': 'province_eng',
        'เขต_อำเภอ': 'district_thai',
        'เขต_อำเภอ_อังกฤษ': 'district_eng',
        'แขวง_ตำบล': 'subdistrict_thai',
        'แขวง_ตำบล_อังกฤษ': 'subdistrict_eng',
        'สถานะโครงการ': 'project_status',
        'object พิกัดของโครงการ': 'project_location',
        'ละติจูดของโตรงการ': 'lat',
        'ลองจิจูดของโตรงการ': 'lon',
        'พิกัดของโครงการ': 'geom',
        'array รายการสัญญา': 'contract',
        'เลขนิติบุคคล13หลัก': 'winner_tin',
        'ผู้ชนะการเสนอราคา': 'winner',
        'เลขที่สัญญา': 'contract_no',
        'วันที่ลงนามในสัญญา': 'contract_date',
        'วันที่สิ้นสุดสัญญา': 'contract_finish_date',
        'งบประมาณในสัญญา_บาท': 'price_agree',
        'สถานะสัญญา': 'status'
    }
    )
    col_name = ['project_id', 'project_name', 'project_type_name', 'dept_name',
        'dept_sub_name', 'purchase_method_name', 'purchase_method_group_name',
        'announce_date', 'project_money', 'price_build', 'sum_price_agree',
        'budget_year', 'transaction_date', 'province_thai', 'province_eng',
        'district_thai', 'district_eng', 'subdistrict_thai', 'subdistrict_eng',
        'project_status', 'geom', 'lat', 'lon', 'winner_tin', 'winner',
        'contract_no', 'contract_date', 'contract_finish_date', 'price_agree',
        'status']
    df = df[col_name]
    df["year"] = year

    job_config_cgd_contract = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(col_name[0], "INT64"),
            bigquery.SchemaField(col_name[1], "STRING"),
            bigquery.SchemaField(col_name[2], "STRING"),
            bigquery.SchemaField(col_name[3], "STRING"),
            bigquery.SchemaField(col_name[4], "STRING"),
            bigquery.SchemaField(col_name[5], "STRING"),
            bigquery.SchemaField(col_name[6], "STRING"),
            bigquery.SchemaField(col_name[7], "STRING"),
            bigquery.SchemaField(col_name[8], "FLOAT64"),
            bigquery.SchemaField(col_name[9], "FLOAT64"),
            bigquery.SchemaField(col_name[10], "FLOAT64"),
            bigquery.SchemaField(col_name[11], "INT64"),
            bigquery.SchemaField(col_name[12], "STRING"),
            bigquery.SchemaField(col_name[13], "STRING"),
            bigquery.SchemaField(col_name[14], "STRING"),
            bigquery.SchemaField(col_name[15], "STRING"),
            bigquery.SchemaField(col_name[16], "STRING"),
            bigquery.SchemaField(col_name[17], "STRING"),
            bigquery.SchemaField(col_name[18], "STRING"),
            bigquery.SchemaField(col_name[19], "STRING"),
            bigquery.SchemaField(col_name[20], "STRING"),
            bigquery.SchemaField(col_name[21], "FLOAT64"),
            bigquery.SchemaField(col_name[22], "FLOAT64"),
            bigquery.SchemaField(col_name[23], "STRING"),
            bigquery.SchemaField(col_name[24], "STRING"),
            bigquery.SchemaField(col_name[25], "STRING"),
            bigquery.SchemaField(col_name[26], "STRING"),
            bigquery.SchemaField(col_name[27], "STRING"),
            bigquery.SchemaField(col_name[28], "FLOAT64"),
            bigquery.SchemaField(col_name[29], "STRING"),
            bigquery.SchemaField("year", "INT64"),
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    df.to_csv(f"./data_clean/{csv_name}", index=False)

    def delete_table_in_dataset(table_id):
        client.delete_table(table_id, not_found_ok=True)  # Make an API request.
        print("Deleted table '{}'.".format(table_id))


    def upload_csv_to_dataset(table_id, file_path, job_config):

        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        while job.state != "DONE":
            time.sleep(2)
            job.reload()
            print(job.state)

        job.result()


    table_id = f"one-for-all-370004.governance.{table_name}"
    file_path = f"./data_clean/{csv_name}"

    # if delete_table:
    #     delete_table_in_dataset(table_id)
    upload_csv_to_dataset(table_id, file_path, job_config_cgd_contract)