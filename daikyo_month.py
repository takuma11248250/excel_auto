# -*- coding: utf-8 -*-
import gspread
import json
import csv
import pandas as pd
import numpy as np
import os
from oauth2client.service_account import ServiceAccountCredentials 
from datetime import date
from dateutil.relativedelta import relativedelta
import openpyxl

#----------現在のディレクトリを取得----------
cur_dir = os.getcwd() 

#----------パスでファイルを特定し取得する----------
webantena_all_path = cur_dir + "\webantenna_all.csv"
webantena_ad_path = cur_dir + "\webantenna_ad.csv"
target_area_path = cur_dir + "\査定対象エリア.csv"

def month_get():
    today = date.today()
    month_ago = today - relativedelta(months=1)
    return str(month_ago.year)+"_"+str(month_ago.month)+"_"+"logfile"

def excel_log(target_df, target_sheetname):
    with pd.ExcelWriter("month_log/"+month_get()+".xlsx", engine="openpyxl", mode="a") as ew:
        target_df.to_excel(ew, sheet_name= target_sheetname, index=False)

def excel_fix(path):
    if path == webantena_all_path:
        global webantena_all_df 
        webantena_all_df = pd.read_csv(path, encoding="shift-jis")
        webantena_all_df = webantena_all_df[['流入種別', '媒体/検索エンジン/流入元サイト', 'キャンペーン名', 'CV時刻', 'CV名', '所在地']]
        webantena_all_df.rename(columns={'媒体/検索エンジン/流入元サイト':'媒体', 'キャンペーン名': 'キャンペーン'}, inplace=True)
        excel_log(webantena_all_df, "ALL")

    elif path == webantena_ad_path:
        global webantena_ad_df
        webantena_ad_df = pd.read_csv(path, encoding="shift-jis")
        webantena_ad_df = webantena_ad_df[['流入種別', '媒体', 'キャンペーン名', 'CV時刻', 'CV名', '所在地']]
        webantena_ad_df.rename(columns={'キャンペーン名': 'キャンペーン'}, inplace=True)
        excel_log(webantena_ad_df, "AD")

def target_area_list():
    with open(target_area_path) as fp:
        return fp.read().splitlines()

#----------メイン関数(以下からのスクリプトが実行される)----------
if __name__ == "__main__":
    #ログを残すExcelファイル作成
    wb = openpyxl.Workbook()
    wb.save("month_log/"+month_get()+".xlsx")

    #「すべての流入種別」「広告のみ」でそれぞれ整形、取得
    excel_fix(webantena_all_path)
    excel_fix(webantena_ad_path)
    
    #データセット結合
    cv_concat_df = pd.concat([webantena_all_df, webantena_ad_df])
    #CV時刻を基準に「広告のみ」レポートで上書き
    cv_concat_df = cv_concat_df.drop_duplicates(['CV時刻'], keep='last')
    #所在地Nanは東京都練馬区とする(すべて「全体お問い合わせ」のため)
    cv_concat_df = cv_concat_df.fillna({"所在地": "東京都練馬区"})
    excel_log(cv_concat_df, "SUM")

    #査定外エリアを除外するため、データセットを新規作成
    fix_cv_concat_df = pd.DataFrame(columns=['流入種別', '媒体', 'キャンペーン', 'CV時刻', 'CV名', '所在地'])
    for area_divide in cv_concat_df.itertuples():
        for target_area in target_area_list():
            if target_area in area_divide.所在地:
                set_targetarea = pd.Series([area_divide.流入種別, area_divide.媒体, area_divide.キャンペーン, area_divide.CV時刻, area_divide.CV名, area_divide.所在地], index=fix_cv_concat_df.columns, name=area_divide.Index)
                fix_cv_concat_df = fix_cv_concat_df.append(set_targetarea)
                continue

    #流入種別Nanはその他流入とする
    fix_cv_concat_df = fix_cv_concat_df.fillna({"流入種別": "その他流入"})
    excel_log(fix_cv_concat_df, "after_df")
    
    #自然検索
    organic_cv_df = fix_cv_concat_df[(fix_cv_concat_df["流入種別"] == ("自然検索")) & (~fix_cv_concat_df["CV名"].str.contains("6 全体お問い合わせ"))] 
    organic_cv = len(organic_cv_df)
    print ("自然検索CV数")
    print ("-----------------")
    print (organic_cv)
    print ("-----------------")

    #直接・メール
    direct_mail_df = fix_cv_concat_df[(fix_cv_concat_df["流入種別"].str.contains("メール|その他流入")) & (~fix_cv_concat_df["CV名"].str.contains("6 全体お問い合わせ"))]
    direct_mail_cv = len(direct_mail_df)
    
    #プロモーションの直接行きCV
    ad_direct_df = fix_cv_concat_df[(fix_cv_concat_df["流入種別"].str.contains("リスティング|バナー|テキスト")) & (fix_cv_concat_df["媒体"].str.contains("自社・関連サイト|IESHIL")) & (~fix_cv_concat_df["CV名"].str.contains("6 全体お問い合わせ"))]
    ad_direct_cv = len(ad_direct_df)

    #直接・メール合算
    direct_mail_cv += ad_direct_cv
    print ("直接・メールCV数")
    print ("-----------------")
    print (direct_mail_cv)
    print ("-----------------")

    #全体お問い合わせ(広告経由)
    ad_allrequest_df = fix_cv_concat_df[(fix_cv_concat_df["流入種別"].str.contains("リスティング|バナー|テキスト")) & (fix_cv_concat_df["CV名"] == ("6 全体お問い合わせ"))]
    ad_allrequest_cv = len(ad_allrequest_df)
    print ("全体お問い合わせ(広告経由)CV数")
    print ("-----------------")
    print (ad_allrequest_cv)
    print ("-----------------")

    #全体お問い合わせ(広告以外)
    allrequest_df = fix_cv_concat_df[(fix_cv_concat_df["流入種別"].str.contains("自然検索|メール|その他流入")) & (fix_cv_concat_df["CV名"] == ("6 全体お問い合わせ"))]
    allrequest_cv = len(allrequest_df)
    print ("全体お問い合わせ(広告以外)CV数")
    print ("-----------------")
    print (allrequest_cv)
    print ("-----------------")

    #プロモーションのリースバック
    ad_leaseback_df = fix_cv_concat_df[(fix_cv_concat_df["流入種別"].str.contains("リスティング|バナー|テキスト")) & (fix_cv_concat_df["キャンペーン"].str.contains("リースバック")) & (~fix_cv_concat_df["CV名"].str.contains("6 全体お問い合わせ", na=False) & (~fix_cv_concat_df["媒体"].str.contains("自社・関連サイト|IESHIL", na=False)))]
    ad_leaseback_cv = len(ad_leaseback_df)
    print ("リースバックCV数")
    print ("-----------------")
    print (ad_leaseback_cv)
    print ("-----------------")

    #プロモーションの注力エリア
    focus_area = '東京都千代田区|東京都中央区|東京都港区|東京都新宿区|東京都渋谷区|東京都文京区|東京都江戸川区|東京都葛飾区|東京都足立区|東京都江東区|東京都墨田区|東京都荒川区|東京都台東区|東京都北区|東京都板橋区|東京都豊島区|東京都練馬区|東京都中野区|東京都杉並区|東京都世田谷区|東京都目黒区|東京都品川区|東京都大田区|神奈川県川崎市|神奈川県横浜市'
    ad_focus_df = fix_cv_concat_df[(fix_cv_concat_df["流入種別"].str.contains("リスティング|バナー|テキスト")) & (fix_cv_concat_df["所在地"].str.contains(focus_area)) & (~fix_cv_concat_df["CV名"].str.contains("6 全体お問い合わせ", na=False)) & (~fix_cv_concat_df['キャンペーン'].str.contains("リースバック", na=False) & (~fix_cv_concat_df["媒体"].str.contains("自社・関連サイト|IESHIL", na=False)))]
    ad_focus_cv = len(ad_focus_df)
    print ("注力エリアCV数")
    print ("-----------------")
    print (ad_focus_cv)
    print ("-----------------")

    #プロモーションの注力エリア、リースバック以外
    ad_other_df = fix_cv_concat_df[(fix_cv_concat_df["流入種別"].str.contains("リスティング|バナー|テキスト")) & (~fix_cv_concat_df["所在地"].str.contains(focus_area, na=False)) & (~fix_cv_concat_df["CV名"].str.contains("6 全体お問い合わせ", na=False)) & (~fix_cv_concat_df['キャンペーン'].str.contains("リースバック", na=False) & (~fix_cv_concat_df["媒体"].str.contains("自社・関連サイト|IESHIL", na=False)))]
    ad_other_cv = len(ad_other_df)
    print ("プロモーションCV数")
    print ("-----------------")
    print (ad_other_cv)
    print ("-----------------")

    #合計CV
    month_all_cv = organic_cv + direct_mail_cv + ad_allrequest_cv + allrequest_cv + ad_leaseback_cv + ad_focus_cv + ad_other_cv
    print ("合計CV数")
    print ("-----------------")
    print (month_all_cv)
    print ("-----------------")

    #----------スプレッドシートの操作----------
    
    #----------スプレッドシートapiの情報を取得----------
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    
    #----------ローカルにあるjsonファイルを特定し、キーを取得----------
    credentials = ServiceAccountCredentials.from_json_keyfile_name('kono-project-272306-4646b029f224.json', scope)

    #----------操作するスプレッドシートを指定----------
    gc = gspread.authorize(credentials)
    SPREADSHEET_KEY = '1IXAcBwm8JSjs_nh9WDLyGqn54uMVlwiiIVn9pP2eHtA'
    workbook = gc.open_by_key(SPREADSHEET_KEY)

    #サマリシートを取得し、それぞれCV数を記載
    samari_worksheet = workbook.worksheet('サマリ')
    
    #自然検索
    organic_cell = samari_worksheet.find('自然検索')
    samari_worksheet.update_acell("B"+str(organic_cell.row), organic_cv)

    #直接・メール
    direct_mail_cell = samari_worksheet.find('直接・メール')
    samari_worksheet.update_acell("B"+str(direct_mail_cell.row), direct_mail_cv)

    #プロモーション
    ad_other_cell = samari_worksheet.find('プロモーション')
    samari_worksheet.update_acell("B"+str(ad_other_cell.row), ad_other_cv)

    #注力エリア
    ad_focus_cell = samari_worksheet.find('注力エリア')
    samari_worksheet.update_acell("B"+str(ad_focus_cell.row), ad_focus_cv)

    #リースバック
    ad_leaseback_cell = samari_worksheet.find('リースバック')
    samari_worksheet.update_acell("B"+str(ad_leaseback_cell.row), ad_leaseback_cv)

    #全体お問い合わせ(広告経由)
    ad_allrequest_cell = samari_worksheet.find('全体お問い合わせ(広告経由)')
    samari_worksheet.update_acell("B"+str(ad_allrequest_cell.row), ad_allrequest_cv)
    
    #全体お問い合わせ(広告以外)
    allrequest_cell = samari_worksheet.find('全体お問い合わせ(広告以外)')
    samari_worksheet.update_acell("B"+str(allrequest_cell.row), allrequest_cv)
    
