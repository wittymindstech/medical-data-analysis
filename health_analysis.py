import os
import boto3
import pandas as pd


AWS_S3_BUCKET = "datanalytics"
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_SESSION_TOKEN = "123"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

pd.set_option("display.max_rows", None)

response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key="text/blood_data_conv.csv")

status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    hb_df = pd.read_csv(response.get("Body"), delimiter = ",")
    df_reset=hb_df.reset_index()
    print(hb_df)
    print(hb_df.columns.tolist())

    hb_df.columns = ((hb_df.columns.str).replace("^ ","")).str.replace(" $","")
    print(hb_df.columns)

    name_female=hb_df.loc[(hb_df['Gender'] == "f")  & (hb_df['Hb (g/dl)'] < 13) & ((hb_df['BILIRUBIN (mg/dL)'] < 0.3) | (hb_df['BILIRUBIN (mg/dL)'] > 1.2))]
    print("********UNHEALTHY FEMALE MEMBERS***********")
    print(name_female)

    name=hb_df.loc[(hb_df['Gender'] == "m")  & (hb_df['Hb (g/dl)'] < 12) & ((hb_df['BILIRUBIN (mg/dL)'] < 0.3) | (hb_df['BILIRUBIN (mg/dL)'] > 1.2))]
    print("**********UNHEALTHY MALE MEMBERS******************")
    print(name)
    #for i, j in hb_df.iterrows():
    #   print(j['NAME'])

else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")
~                                                                   
