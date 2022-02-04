import csv
import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os


def convert(url):
    with open('config/google-creds.json','r') as rf:
        gcreds = json.load(rf)
    api_key = gcreds["API_KEY"]
    spreadsheet_id = "1453eQwV7paPERbXzm0KXM8W7Byww4msPkeRjbzyQUCE"
    try:
        # creds = Credentials.from_authorized_user_file('config/google-creds.json', SCOPES)
        service = build('sheets', 'v4', developerKey=api_key)

        # Call the Sheets API
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_name = sheet_metadata['properties']["title"]
        sheets = sheet_metadata.get('sheets', '')
        for sheet in sheets:
            ranges = [sheet['properties']['title']]
            result = service.spreadsheets().values().batchGet(spreadsheetId=spreadsheet_id, ranges=ranges).execute()
            if os.path.isdir(sheet_name):
                pass
            else:
                os.mkdir(sheet_name)
            filename = sheet_name + '/' + ranges[0] + '.csv'
            data = result["valueRanges"][0]["values"]
            with open(filename,'w',encoding='utf-8-sig') as wf:
                writer = csv.writer(wf)
                writer.writerows(data)

        # sheet = service.open_by_key(spreadsheet_id)
        # print(sheet)
    except HttpError as err:
        print(err)

if __name__ == '__main__':
    url = 'https://docs.google.com/spreadsheets/d/1453eQwV7paPERbXzm0KXM8W7Byww4msPkeRjbzyQUCE/edit?usp=sharing'
    convert(url)
    