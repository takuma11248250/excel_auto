# -*- coding: utf-8 -*-
import subprocess
import os
import csv
import pprint
import openpyxl
import pandas as pd
import codecs
import operator
import numpy as np

GSS_path = r"C:\Users\takuma_kono\Desktop\MRESreport\スプレッドシート貼付用レポート.csv"
GSS_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\google_risting.csv"
G_DSA_path = r"C:\Users\takuma_kono\Desktop\MRESreport\【DSA】スプレッドシート貼付用レポート.csv"
G_DSA_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\google_DSA.csv"
GDN_path = r"C:\Users\takuma_kono\Desktop\MRESreport\【GDN】スプレッドシート貼付用レポート.csv"
GDN_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\google_display.csv"
YSS_path = r"C:\Users\takuma_kono\Desktop\MRESreport\【YSS】スプレッドシート貼付用レポート.csv"
YSS_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\yahoo_risting.csv"
Y_DSA_path = r"C:\Users\takuma_kono\Desktop\MRESreport\【DSA】検索クエリーレポート.csv"
Y_DSA_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\yahoo_DSA.csv"
YDN_path = r"C:\Users\takuma_kono\Desktop\MRESreport\【YDN】スプレッドシート貼付用レポート.csv"
YDN_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\yahoo_display.csv"
brokerage_path = r"C:\Users\takuma_kono\Desktop\MRESreport\流通.csv"
brokerage_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\brokerage.csv"
office_building_pm_path = r"C:\Users\takuma_kono\Desktop\MRESreport\ビル運営.csv"
office_building_pm_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\office_building_pm.csv"
appraisal_path = r"C:\Users\takuma_kono\Desktop\MRESreport\不動産鑑定.csv"
appraisal_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\appraisal.csv"
mansion_pm_path = r"C:\Users\takuma_kono\Desktop\MRESreport\住宅運営.csv"
mansion_pm_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\mansion_pm.csv"
office_building_lend_path = r"C:\Users\takuma_kono\Desktop\MRESreport\オフィス賃貸.csv"
office_building_lend_path_w = r"C:\Users\takuma_kono\Desktop\MRESreport\office_building_lend.csv"

def G_excel_fix(path, path_w):
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
    df.to_csv(path_w, index=False)

def Y_excel_fix(path, path_w):
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
    df.to_csv(path_w, index=False)

def GA_fix(path, path_w):
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
    df.to_csv(path_w, index=False)


if __name__ == "__main__":
    G_excel_fix(GDN_path, GDN_path_w)
    G_excel_fix(GSS_path, GSS_path_w)
    G_excel_fix(G_DSA_path, G_DSA_path_w)

    Y_excel_fix(YSS_path, YSS_path_w)
    Y_excel_fix(YDN_path, YDN_path_w)
    Y_excel_fix(Y_DSA_path, Y_DSA_path_w)

    GA_fix(brokerage_path, brokerage_path_w)
    GA_fix(office_building_pm_path, office_building_pm_path_w)
    GA_fix(appraisal_path, appraisal_path_w)
    GA_fix(mansion_pm_path, mansion_pm_path_w)
    GA_fix(office_building_lend_path, office_building_lend_path_w)