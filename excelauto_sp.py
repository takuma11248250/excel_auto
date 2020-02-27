# -*- coding: utf-8 -*-
import datetime
from typing import Dict, List, Union

import pandas as pd
import requests
from apiclient import discovery
from oauth2client.client import OAuth2Credentials
import subprocess
import os
import csv
import pprint
import openpyxl
import pandas as pd
import codecs
import operator
import numpy as np
import os

cur_dir = os.getcwd() 

# パス指定
GSS_path = cur_dir + "\スプレッドシート貼付用レポート.csv"
G_DSA_path = cur_dir + "\【DSA】スプレッドシート貼付用レポート.csv"
GDN_path = cur_dir + "\【GDN】スプレッドシート貼付用レポート.csv"
YSS_path = cur_dir + "\【YSS】スプレッドシート貼付用レポート.csv"
Y_DSA_path = cur_dir + "\【DSA】検索クエリーレポート.csv"
YDN_path = cur_dir + "\【YDN】スプレッドシート貼付用レポート.csv"
brokerage_path = cur_dir + "\流通.csv"
office_building_pm_path = cur_dir + "\ビル運営.csv"
appraisal_path = cur_dir + "\不動産鑑定.csv"
mansion_pm_path = cur_dir + "\住宅運営.csv"
office_building_lend_path = cur_dir + "\オフィス賃貸.csv"

# 認証情報
CLIENT_ID = "412303833432-mn7lblvks1laouucgis7ub7qlg3peivk.apps.googleusercontent.com"
CLIENT_SECRET = "clOUEfM9C3GBnIbFB3fcktyc"
REFRESH_TOKEN = "1//0eTaYBZ_tnYjYCgYIARAAGA4SNwF-L9Irqr1Ws2lWDml8qFw0VNZoH1EH96guD95afHcOKcWhpyOdeRZR-YP8ZE6MYEUMD3TSQVA"


class MySpreadsheet:
    """Google Spreadsheet を操作します."""

    def __init__(self):
        """初期化. OAuth認証もここで行います."""
        refresh_params = {
            "client_id": CLIENT_ID,
            "refresh_token": REFRESH_TOKEN,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token"}

        rs = requests.post('https://accounts.google.com/o/oauth2/token',
                           refresh_params).json()
        expiry = datetime.datetime.now() + datetime.timedelta(seconds=rs['expires_in'])

        cred_params = {
            'access_token': rs['access_token'],
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'refresh_token': REFRESH_TOKEN,
            'token_expiry': expiry,
            'token_uri': 'https://accounts.google.com/o/oauth2/token',
            'user_agent': ''}

        self.credentials = OAuth2Credentials(**cred_params)

    def create(self, title: str) -> Dict[str, Union[str, List[str]]]:
        """スプレッドシートを新規作成します.

        :param title: スプレッドシートのタイトル.
        :return: スプレッドシートの属性.
        """
        service = discovery.build('sheets', 'v4', credentials=self.credentials)
        body = {"properties": {"title": title}}
        request = service.spreadsheets().create(body=body)
        response = request.execute()

        return {"id": response["spreadsheetId"],
                "url": response["spreadsheetUrl"],
                "sheets": [x["properties"]["title"] for x in response["sheets"]]}

    def read(self, spreadsheet_id: int, sheet_name: str, sheet_range: str,
             header: bool = True) -> pd.DataFrame:
        """スプレッドシートのテーブルを読込み、pandas DataFrame にして返します.

        :param spreadsheet_id: スプレッドシート ID.
        :param sheet_name: シートの名前.
        :param sheet_range: 読み込む範囲. 例) "A2:C" A2からC行のデータが続くまで.
        :param header: 先頭行は各カラム名ならTrueを指定してください.
        :return: 読み込んだテーブルの情報が入った DataFrame.
        """
        service = discovery.build('sheets', 'v4', credentials=self.credentials)
        _range = f"{sheet_name}!{sheet_range}"
        request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                      range=_range,
                                                      valueRenderOption="UNFORMATTED_VALUE")
        response = request.execute()
        values = response["values"]

        if header:
            columns = values[0]
            values = values[1:]
        else:
            columns = None

        return pd.DataFrame(values, columns=columns)

    def update(self, spreadsheet_id: int, sheet_name: str, sheet_range: str,
               df: pd.DataFrame, header: bool = False) -> None:
        """スプレッドシートのテーブルを更新します.

        :param spreadsheet_id: スプレッドシート ID.
        :param sheet_name: シートの名前.
        :param sheet_range: 読み込む範囲. 例) "A2" 更新する範囲がA2から始まっている.
        :param df: 更新する情報が入っている DataFrame.
        :param header: カラム名を書き込む場合は True を設定してください.
        """
        _range = f"{sheet_name}!{sheet_range}"
        if header:
            dat = [df.columns.tolist()] + df.values.tolist()
        else:
            dat = df.values.tolist()
        body = {"values": dat}

        value_input_option = "USER_ENTERED"
        service = discovery.build('sheets', 'v4', credentials=self.credentials)
        request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id,
                                                         valueInputOption=value_input_option,
                                                         range=_range,
                                                         body=body)
        request.execute()

    def append(self, spreadsheet_id: str, sheet_name: str, sheet_range: str,
               df: pd.DataFrame, header: bool = False) -> None:
        """スプレッドシートのテーブルに情報を加えます.

        :param spreadsheet_id: スプレッドシート ID.
        :param sheet_name: シートの名前.
        :param sheet_range: 読み込む範囲. 例) "A2" 更新するテーブルはA2から始まっている.
        :param df: 更新する情報が入っている DataFrame.
        :param header: カラム名を書き込む場合は True を設定してください.
        """
        _range = f"{sheet_name}!{sheet_range}"
        value_input_option = "USER_ENTERED"
        if header:
            dat = [df.columns.tolist()] + df.values.tolist()
        else:
            dat = df.values.tolist()
        body = {"values": dat}

        service = discovery.build('sheets', 'v4', credentials=self.credentials)
        request = service.spreadsheets().values().append(spreadsheetId=spreadsheet_id,
                                                         valueInputOption=value_input_option,
                                                         range=_range,
                                                         body=body)
        request.execute()

    def clear(self, spreadsheet_id: str, sheet_name: str, sheet_range: str) -> None:
        """スプレッドシートのテーブルのうち、指定範囲の情報を削除します.

        :param spreadsheet_id: スプレッドシート ID.
        :param sheet_name: シートの名前.
        :param sheet_range: 読み込む範囲. 例) "A:Z" A列からZ列の全ての情報を削除.
        """
        _range = f"{sheet_name}!{sheet_range}"
        body = {}
        service = discovery.build('sheets', 'v4', credentials=self.credentials)
        request = service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,
                                                        range=_range,
                                                        body=body)
        request.execute()



def G_excel_fix(path):
    global df
    if path == r"C:\Users\takuma_kono\Desktop\MRESreport\【DSA】スプレッドシート貼付用レポート.csv":
        df = pd.read_csv(path, skiprows=2, usecols=lambda x: x not in ['週', '動的広告ターゲット', '検査語句のマッチ　タイプ', '通貨', 'クリック率', '平均クリック単価', 'コンバージョン', 'コンバージョン率', 'コンバージョン単価'])
        budget = df['費用']/0.8
    else:
        df = pd.read_csv(path, skiprows=2, usecols=lambda x: x not in ['週', '通貨', 'クリック率', '平均クリック単価', 'コンバージョン', 'コンバージョン率', 'コンバージョン単価'])
    if path == r"C:\Users\takuma_kono\Desktop\MRESreport\【GDN】スプレッドシート貼付用レポート.csv":
        df = df[df['キャンペーン'].str.contains('リマ|カスタム')]
    if not path == r"C:\Users\takuma_kono\Desktop\MRESreport\【DSA】スプレッドシート貼付用レポート.csv":
        df_imp = (df['表示回数'].str.replace(',', '').astype(float).astype(int)) 
        df['表示回数'] = df_imp
        cost = (df['費用'].str.replace(',', '').astype(float).astype(int))
        budget = cost/0.8
    df['予算'] = budget
    df = df.drop('費用', axis=1)
    if path == r"C:\Users\takuma_kono\Desktop\MRESreport\【GDN】スプレッドシート貼付用レポート.csv":
        df = df[['キャンペーン', '広告グループ', '予算', '表示回数', 'クリック数']]
    elif path == r"C:\Users\takuma_kono\Desktop\MRESreport\スプレッドシート貼付用レポート.csv":
        df['キーワード'] = df['検索キーワード']
        df = df[['キャンペーン', '広告グループ', 'キーワード', '予算', '表示回数', 'クリック数']]
    elif path == r"C:\Users\takuma_kono\Desktop\MRESreport\【DSA】スプレッドシート貼付用レポート.csv":
        df['キーワード'] = df['検索語句']
        df = df[['キャンペーン', '広告グループ', 'キーワード', '予算', '表示回数', 'クリック数']]
    df = df.sort_values('キャンペーン')

def Y_excel_fix(path):
    global df
    if path == r"C:\Users\takuma_kono\Desktop\MRESreport\【YSS】スプレッドシート貼付用レポート.csv":
        df = pd.read_csv(path,  encoding="shift-jis", usecols=lambda x: x not in ['毎月', 'マッチタイプ'])
    elif path == r"C:\Users\takuma_kono\Desktop\MRESreport\【DSA】検索クエリーレポート.csv":
        df = pd.read_csv(path,  encoding='cp932', usecols=lambda x: x not in ['毎月', '検索クエリ―のマッチタイプ', 'キャンペーンID'])
    elif path == r"C:\Users\takuma_kono\Desktop\MRESreport\【YDN】スプレッドシート貼付用レポート.csv":
        df = pd.read_csv(path,  encoding='cp932', usecols=lambda x: x not in ['日', '平均掲載順位'])
        df_cam = (df['キャンペーン名'].str.replace('20190501_', ''))
        df['キャンペーン名'] = df_cam
    last_row = df.tail(1).index[0]
    df = df.drop(last_row)
    budget = df['コスト']/0.8
    df['予算'] = budget
    df = df.drop('コスト', axis=1)
    df['キャンペーン'] = df['キャンペーン名']
    df['広告グループ'] = df['広告グループ名']
    if path == r"C:\Users\takuma_kono\Desktop\MRESreport\【YSS】スプレッドシート貼付用レポート.csv":
        df['表示回数'] = df['インプレッション数']
        df = df[['キャンペーン', '広告グループ', 'キーワード', '予算', '表示回数', 'クリック数']]
    elif path == r"C:\Users\takuma_kono\Desktop\MRESreport\【DSA】検索クエリーレポート.csv":
        df['表示回数'] = df['インプレッション数']
        df['キーワード'] = df['検索クエリー']
        df = df[['キャンペーン', '広告グループ', 'キーワード', '予算', '表示回数', 'クリック数']]
    elif path == r"C:\Users\takuma_kono\Desktop\MRESreport\【YDN】スプレッドシート貼付用レポート.csv":
        df['表示回数'] = df['インプレッション数（旧）']
        df = df[['キャンペーン', '広告グループ', '予算', '表示回数', 'クリック数']]
    df = df.sort_values('キャンペーン')

def GA_fix(path):
    global df
    df = pd.read_csv(path,  skiprows=6, usecols=lambda x: x not in ['ユーザー', '新規ユーザー', '直帰率', 'ページ/セッション', '平均セッション時間', '問い合わせ／不動産（目標 1 のコンバージョン率）', '問い合わせ／不動産（目標 1 の完了数）', '問い合わせ／不動産（目標 1 の値）'])
    df = df.dropna(how='any')
    df['キャンペーン'] = df['広告のコンテンツ']
    for campaign in df['キャンペーン']:
        if campaign == '001':
            df['キャンペーン'] = df['キャンペーン'].str.replace('001', '001_社名【流通】')
        elif campaign == '020_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('020_1', '020_DSA【流通】')
        elif campaign == '021_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('021_1', '021_DSA【オフィス賃貸】')
        elif campaign == '022_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('022_1', '022_DSA【ビル運営】')
        elif campaign == '023_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('023_1', '023_DSA【住宅運営】')
        elif campaign == '025_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('025_1', '025_1_DSA【不動産鑑定】')
        elif campaign == '026_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('026_1', '026_不動産売買・投資・CRE戦略【流通】')
        elif campaign == '026_2':
            df['キャンペーン'] = df['キャンペーン'].str.replace('026_2', '026_不動産売買・投資・CRE戦略【流通】')
        elif campaign == '026_3':
            df['キャンペーン'] = df['キャンペーン'].str.replace('026_3', '026_不動産売買・投資・CRE戦略【流通】')
        elif campaign == '027_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('027_1', '027_オフィス賃貸【オフィス賃貸】')
        elif campaign == '027_2':
            df['キャンペーン'] = df['キャンペーン'].str.replace('027_2', '027_オフィス賃貸【オフィス賃貸】')
        elif campaign == '028_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('028_1', '028_ビル運営【ビル運営】')
        elif campaign == '029_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('029_1', '029_住宅運営【住宅運営】')
        elif campaign == '030_1':
            df['キャンペーン'] = df['キャンペーン'].str.replace('030_1', '030_不動産鑑定【不動産鑑定】')

    df['広告グループ'] = df['広告のコンテンツ']
    for group in df['広告グループ']:
        if group == '001':
            df['広告グループ'] = df['広告グループ'].str.replace('001', '001_社名【流通】')
        elif group == '020_1':
            df['広告グループ'] = df['広告グループ'].str.replace('020_1', '020_1_DSA【流通】')
        elif group == '021_1':
            df['広告グループ'] = df['広告グループ'].str.replace('021_1', '021_1_DSA【オフィス賃貸】')
        elif group == '022_1':
            df['広告グループ'] = df['広告グループ'].str.replace('022_1', '022_1_DSA【ビル運営】')
        elif group == '023_1':
            df['広告グループ'] = df['広告グループ'].str.replace('023_1', '023_1_DSA【住宅運営】')
        elif group == '025_1':
            df['広告グループ'] = df['広告グループ'].str.replace('025_1', '025_1_DSA【不動産鑑定】')
        elif group == '026_1':
            df['広告グループ'] = df['広告グループ'].str.replace('026_1', '026_1_不動産売買【流通】')
        elif group == '026_2':
            df['広告グループ'] = df['広告グループ'].str.replace('026_2', '026_2_不動産投資【流通】')
        elif group == '026_3':
            df['広告グループ'] = df['広告グループ'].str.replace('026_3', '026_3_CRE戦略【流通】')
        elif group == '027_1':
            df['広告グループ'] = df['広告グループ'].str.replace('027_1', '027_1_オフィス賃貸【オフィス賃貸】')
        elif group == '027_2':
            df['広告グループ'] = df['広告グループ'].str.replace('027_2', '027_2_オフィスウェル【オフィス賃貸】')
        elif group == '028_1':
            df['広告グループ'] = df['広告グループ'].str.replace('028_1', '028_1_ビル運営【ビル運営】')
        elif group == '029_1':
            df['広告グループ'] = df['広告グループ'].str.replace('029_1', '029_1_住宅運営【住宅運営】')
        elif group == '030_1':
            df['広告グループ'] = df['広告グループ'].str.replace('030_1', '030_1_不動産鑑定【不動産鑑定】')
    df = df.drop('広告のコンテンツ', axis=1)

    df['ソース'] = df['参照元/メディア']
    for source in df['ソース']:
        if source == 'google / cpc':
            df['ソース'] = df['ソース'].str.replace('google / cpc', 'Google')
        elif source == 'yahoo / cpc':
            df['ソース'] = df['ソース'].str.replace('yahoo / cpc', 'Yahoo')
        elif source == 'GDN / display':
            df['ソース'] = df['ソース'].str.replace('GDN / display', 'GDN')
        elif source == 'YDN / display':
            df['ソース'] = df['ソース'].str.replace('YDN / display', 'YDN')
        
    df['メディア'] = df['参照元/メディア']
    for media in df['メディア']:
        if media == 'google / cpc':
            df['メディア'] = df['メディア'].str.replace('google / cpc', 'Paid Search')
        elif media == 'yahoo / cpc':
            df['メディア'] = df['メディア'].str.replace('yahoo / cpc', 'Paid Search')
        elif media == 'GDN / display':
            df['メディア'] = df['メディア'].str.replace('GDN / display', 'Display')
        elif media == 'YDN / display':
            df['メディア'] = df['メディア'].str.replace('YDN / display', 'Display')
    df = df.drop('参照元/メディア', axis=1)

    df['フォーム訪問数'] = df['セッション']
    df = df[['ソース', 'メディア', 'キャンペーン', '広告グループ', 'フォーム訪問数']]

if __name__ == "__main__":
    ms = MySpreadsheet()

    info = ms.create("MRES_rawdata")
    s_id = info["id"]
    s_name = info["sheets"][0]

    G_excel_fix(GSS_path)
    ms.append(s_id, s_name, "A1", df, header=True)
    G_excel_fix(G_DSA_path)
    ms.append(s_id, s_name, "A1", df, header=None)
    G_excel_fix(GDN_path)
    ms.append(s_id, s_name, "A1", df, header=None)
    
    Y_excel_fix(YSS_path)
    ms.append(s_id, s_name, "A1", df, header=True)
    Y_excel_fix(Y_DSA_path)
    ms.append(s_id, s_name, "A1", df, header=None)
    Y_excel_fix(YDN_path)
    ms.append(s_id, s_name, "A1", df, header=None)
    
    GA_fix(brokerage_path)
    ms.append(s_id, s_name, "A1", df, header=True)
    GA_fix(office_building_pm_path)
    ms.append(s_id, s_name, "A1", df, header=None)
    GA_fix(appraisal_path)
    ms.append(s_id, s_name, "A1", df, header=None)
    GA_fix(mansion_pm_path)
    ms.append(s_id, s_name, "A1", df, header=None)
    GA_fix(office_building_lend_path)
    ms.append(s_id, s_name, "A1", df, header=None)
    
