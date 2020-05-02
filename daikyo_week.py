# -*- coding: utf-8 -*-
import gspread
import json
import csv
import pandas as pd
import numpy as np
import os
from oauth2client.service_account import ServiceAccountCredentials 
import datetime
import time
import calendar
import warnings
import logging
from retry import retry

#----------すべての警告を非表示にする----------
warnings.simplefilter('ignore')

#----------現在のディレクトリを取得----------
cur_dir = os.getcwd() 

#----------パスでファイルを特定し取得する----------
G_path = cur_dir + "\G_スプレッドシート貼付用レポート.csv"
G_area_path = cur_dir + "\G_ターゲット地域レポート.csv"
YSS_path = cur_dir + "\YSS_スプレッドシート貼付用レポート.csv"
YSS_area_path = cur_dir + "\YSS_ターゲット地域レポート.csv"
YDN_path = cur_dir + "\YDN_スプレッド貼付用レポート.csv"
YDN_area_path = cur_dir + "\YDN_ターゲット地域レポート.csv"
webantena_path = cur_dir + "\webantenna.csv"
criteo_path = cur_dir + "\Criteo.csv"
facebook_path = cur_dir + "\Facebook.csv"
smartnews_path = cur_dir + "\Smartnews.csv"

#----------それぞれのExcelファイル毎を整形する関数----------
def excel_fix(path):
    if path == G_path:
        G_df = pd.read_csv(path, skiprows=2, usecols=lambda x: x not in ['通貨'])
        G_df.rename(columns={'平均クリック単価': 'クリック単価'}, inplace=True)
        G_df.dropna(how='all', inplace=True)
        cost = (G_df['費用'].str.replace(',', '').astype(float).astype(int))
        budget = cost/0.8
        G_df.drop('費用', axis=1, inplace=True)
        G_df.insert(1, "費用", budget)
        
        #社名・ブランド名GSS
        GSS_df_brand = G_df[G_df['キャンペーン'].str.contains('社名・ブランド名') & ~G_df['キャンペーン'].str.contains('購入')]
        sum_imp = GSS_df_brand["表示回数"].str.replace(',', '').astype("int64")
        if G_df["クリック数"].dtypes == "object":
            sum_click = GSS_df_brand["クリック数"].str.replace(',', '').astype("int64")
        else:
            sum_click = GSS_df_brand["クリック数"].astype("int64")
        sum_imp = sum_imp.sum()
        sum_click = sum_click.sum()
        sum_budget = GSS_df_brand["費用"].sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global GSS_df_brand_result
        GSS_df_brand_result = pd.DataFrame([["Google", "YG/社名ブランド名", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=list(colum))
        logger.log("社名・ブランド名GSS")
        logger.log("-----------------------")
        logger.log(GSS_df_brand)
        logger.log("-----------------------")
        
        #リースバックGSS
        GSS_df_leaseback_brand = G_df[(G_df['キャンペーン'] == ('3-2_【リースバック】一般・エリア')) | (G_df['キャンペーン'] == ('3-3_【リースバック】単体ワード')) | (G_df['キャンペーン'] == ('3-1_【リースバック】社名・ブランド名'))]
        sum_imp = GSS_df_leaseback_brand["表示回数"].str.replace(',', '').astype("int64")
        if G_df["クリック数"].dtypes == "object":
            sum_click = GSS_df_leaseback_brand["クリック数"].str.replace(',', '').astype("int64")
        else:
            sum_click = GSS_df_leaseback_brand["クリック数"].astype("int64")
        sum_imp = sum_imp.sum()
        sum_click = sum_click.sum()
        sum_budget = GSS_df_leaseback_brand["費用"].sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global GSS_df_leaseback_brand_result
        GSS_df_leaseback_brand_result = pd.DataFrame([["Google", "YG/リースバック", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=list(colum))
        logger.log("リースバックGSS")
        logger.log("-----------------------")
        logger.log(GSS_df_leaseback_brand)
        logger.log("-----------------------")
        
        #その他エリアGSS
        GSS_df_otherarea_brand = G_df[(G_df['キャンペーン'] == ('2-2_【売却／その他】一般・エリア')) | (G_df['キャンペーン'] == ('2-1_【売却／その他】社名・ブランド名'))]
        sum_imp = GSS_df_otherarea_brand["表示回数"].str.replace(',', '').astype("int64")
        if G_df["クリック数"].dtypes == "object":
            sum_click = GSS_df_otherarea_brand["クリック数"].str.replace(',', '').astype("int64")
        else:
            sum_click = GSS_df_otherarea_brand["クリック数"].astype("int64")
        sum_imp = sum_imp.sum()
        sum_click = sum_click.sum()
        sum_budget = GSS_df_otherarea_brand["費用"].sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global GSS_df_otherarea_brand_result
        GSS_df_otherarea_brand_result = pd.DataFrame([["Google", "YG/その他エリア", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=list(colum))
        logger.log ("その他エリアGSS")
        logger.log ("-----------------------")
        logger.log (GSS_df_otherarea_brand)
        logger.log ("-----------------------")
        
        #リースバックGDNリマケ
        global GDN_df_leaseback_rm
        GDN_df_leaseback_rm = G_df[(G_df['キャンペーン'] == ('【リースバック／GDN】リマーケティング')) | (G_df['キャンペーン'] == ('【リースバック／GDN】リマーケティング_フォーム到達者'))]
        GDN_df_leaseback_rm['表示回数'] = GDN_df_leaseback_rm["表示回数"].str.replace(',', '').astype("int64")
        if G_df["クリック数"].dtypes == "object":
            GDN_df_leaseback_rm["クリック数"] = GDN_df_leaseback_rm["クリック数"].str.replace(',', '').astype("int64")
        logger.log ("リースバックGDNリマケ")
        logger.log ("-----------------------")
        logger.log (GDN_df_leaseback_rm)
        logger.log ("-----------------------")

        #購入GSS
        GSS_df_buy = G_df[G_df['キャンペーン'].str.contains('購入')]
        sum_imp = GSS_df_buy["表示回数"].str.replace(',', '').astype("int64")
        if G_df["クリック数"].dtypes == "object":
            sum_click = GSS_df_buy["クリック数"].str.replace(',', '').astype("int64")
        else:
            sum_click = GSS_df_buy["クリック数"].astype("int64")
        sum_imp = sum_imp.sum()
        sum_click = sum_click.sum()
        sum_budget = GSS_df_buy["費用"].sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global GSS_df_buy_result
        GSS_df_buy_result = pd.DataFrame([["Google", "購入サマリ", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=list(colum))
        logger.log ("購入GSS")
        logger.log ("-----------------------")
        logger.log (GSS_df_buy)
        logger.log ("-----------------------")
        
        
    elif path == G_area_path:
        G_area_df = pd.read_csv(path, skiprows=2, usecols=lambda x: x not in ['入札単価調整比', '追加済み / 除外済み', '通貨コード'])
        G_area_df.rename(columns={'平均クリック単価': 'クリック単価'}, inplace=True)
        delete_row = G_area_df.tail(4).index
        G_area_df.drop(delete_row, inplace=True)
        cost = (G_area_df['費用'].astype(float).astype(int))
        budget = cost/0.8
        G_area_df.drop('費用', axis=1, inplace=True)
        G_area_df.insert(2, "費用", budget)
        
        #都心6区GSS
        GSS_df_six_brand = G_area_df[G_area_df['キャンペーン'].str.contains('1-2_【売却／注力】一般・エリア|1-1_【売却／注力】社名・ブランド名') & G_area_df['地域'].str.contains('東京都') & G_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        sum_imp = GSS_df_six_brand["表示回数"].str.replace(',', '').astype("int64")
        sum_imp = sum_imp.sum()
        if G_area_df["クリック数"].dtypes == "object":
            sum_click = GSS_df_six_brand["クリック数"].str.replace(',', '').astype("int64")
        else:
            sum_click = GSS_df_six_brand["クリック数"].astype("int64")
        sum_click = sum_click.sum()
        sum_budget = GSS_df_six_brand["費用"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global GSS_df_six_brand_result
        GSS_df_six_brand_result = pd.DataFrame([["Google", "YG/都心6区", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("都心6区GSS")
        logger.log ("-----------------------")
        logger.log (GSS_df_six_brand)
        logger.log ("-----------------------")

        #注力エリアGSS
        GSS_df_focus_brand = G_area_df[(G_area_df['キャンペーン'].str.contains('1-2_【売却／注力】一般・エリア|1-1_【売却／注力】社名・ブランド名')) & ~G_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        sum_imp = GSS_df_focus_brand["表示回数"].str.replace(',', '').astype("int64")
        sum_imp = sum_imp.sum()
        if G_area_df["クリック数"].dtypes == "object":
            sum_click = GSS_df_focus_brand["クリック数"].str.replace(',', '').astype("int64")
        else:
            sum_click = GSS_df_focus_brand["クリック数"].astype("int64")
        sum_click = sum_click.sum()
        sum_budget = GSS_df_focus_brand["費用"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global GSS_df_focus_brand_result
        GSS_df_focus_brand_result = pd.DataFrame([["Google", "YG/注力エリア", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("注力エリアGSS")
        logger.log ("-----------------------")
        logger.log (GSS_df_focus_brand)
        logger.log ("-----------------------")
        
        #都心6区リマケ
        GDN_df_six_rm = G_area_df[G_area_df['キャンペーン'].str.contains('【売却／注力／GDN】') & G_area_df['キャンペーン'].str.contains('リマーケティング|全来訪者向け')  & (G_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区'))]
        sum_imp = GDN_df_six_rm["表示回数"].str.replace(',', '').astype("int64")
        sum_imp = sum_imp.sum()
        if G_area_df["クリック数"].dtypes == "object":
            sum_click = GDN_df_six_rm["クリック数"].str.replace(',', '').astype("int64")
        else:
            sum_click = GDN_df_six_rm["クリック数"].astype("int64")
        sum_click = sum_click.sum()
        sum_budget = GDN_df_six_rm["費用"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global GDN_df_six_rm_result
        GDN_df_six_rm_result = pd.DataFrame([["GDNRM", "YG/都心6区", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("都心6区リマケ")
        logger.log ("-----------------------")
        logger.log (GDN_df_six_rm)
        logger.log ("-----------------------")
        
        #注力エリアリマケ
        GDN_df_focus_rm = G_area_df[G_area_df['キャンペーン'].str.contains('【売却／注力／GDN】') & G_area_df['キャンペーン'].str.contains('リマーケティング|全来訪者向け') & ~G_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        sum_imp = GDN_df_focus_rm["表示回数"].str.replace(',', '').astype("int64")
        sum_imp = sum_imp.sum()
        if G_area_df["クリック数"].dtypes == "object":
            sum_click = GDN_df_focus_rm["クリック数"].str.replace(',', '').astype("int64")
        else:
            sum_click = GDN_df_focus_rm["クリック数"].astype("int64")
        sum_click = sum_click.sum()
        sum_budget = GDN_df_focus_rm["費用"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global GDN_df_focus_rm_result
        GDN_df_focus_rm_result = pd.DataFrame([["GDNRM", "YG/注力エリア", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("注力エリアリマケ")
        logger.log ("-----------------------")
        logger.log (GDN_df_focus_rm)
        logger.log ("-----------------------")

        #その他エリアリマケ
        GDN_df_otherarea_rm = G_area_df[G_area_df['キャンペーン'].str.contains('【売却／通常／GDN】') & G_area_df['キャンペーン'].str.contains('リマーケティング|全来訪者向け') ]
        sum_imp = GDN_df_otherarea_rm["表示回数"].str.replace(',', '').astype("int64")
        sum_imp = sum_imp.sum()
        if G_area_df["クリック数"].dtypes == "object":
            sum_click = GDN_df_otherarea_rm["クリック数"].str.replace(',', '').astype("int64")
        else:
            sum_click = GDN_df_otherarea_rm["クリック数"].astype("int64")
        sum_click = sum_click.sum()
        sum_budget = GDN_df_otherarea_rm["費用"].astype(int).sum()
        sum_click = GDN_df_otherarea_rm["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global GDN_df_otherarea_rm_result
        GDN_df_otherarea_rm_result = pd.DataFrame([["GDNRM", "YG/その他エリア", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("その他エリアリマケ")
        logger.log ("-----------------------")
        logger.log (GDN_df_otherarea_rm)
        logger.log ("-----------------------")
        
    elif path == YSS_path:
        YSS_df = pd.read_csv(path, encoding="shift-jis", usecols=lambda x: x not in ['毎月', 'キャンペーンタイプ'])
        YSS_df.rename(columns={'キャンペーン名': 'キャンペーン', '広告グループ名': '広告グループ', 'コスト': '費用', 'インプレッション数': '表示回数', '平均CPC': 'クリック単価' }, inplace=True)
        YSS_df.dropna(how='all', inplace=True)
        last_row = YSS_df.tail(1).index[0]
        YSS_df.drop(last_row, inplace=True)
        cost = YSS_df['費用']
        budget = cost/0.8
        YSS_df.drop('費用', axis=1, inplace=True)
        YSS_df.insert(1, "費用", budget)
        
        #社名・ブランド名YSS
        YSS_df_brand =YSS_df[YSS_df['キャンペーン'].str.contains('社名・ブランド名') & ~YSS_df['キャンペーン'].str.contains('購入')]
        sum_budget = YSS_df_brand["費用"].astype(int).sum()
        sum_imp = YSS_df_brand["表示回数"].astype(int).sum()
        sum_click = YSS_df_brand["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp 
        sum_cpc = sum_budget/sum_click
        global YSS_df_brand_result
        YSS_df_brand_result = pd.DataFrame([["Yahoo!リスティング", "YG/社名ブランド名", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=list(colum))
        logger.log ("社名・ブランド名YSS")
        logger.log ("-----------------------")
        logger.log (YSS_df_brand)
        logger.log ("-----------------------")
        
        #リースバックYSS
        YSS_df_leaseback_brand = YSS_df[(YSS_df['キャンペーン'] == ('3-2_【リースバック】一般・エリア')) | (YSS_df['キャンペーン'] == ('3-3_【リースバック】単体ワード'))| (YSS_df['キャンペーン'] == ('3-1_【リースバック】社名・ブランド名')) | (YSS_df['キャンペーン'] == ('3-4_【リースバック】DAS'))]
        sum_budget = YSS_df_leaseback_brand["費用"].astype(int).sum()
        sum_imp = YSS_df_leaseback_brand["表示回数"].astype(int).sum()
        sum_click = YSS_df_leaseback_brand["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global YSS_df_leaseback_brand_result
        YSS_df_leaseback_brand_result = pd.DataFrame([["Yahoo!リスティング", "YG/リースバック", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=list(colum))
        logger.log ("リースバックYSS")
        logger.log ("-----------------------")
        logger.log (YSS_df_leaseback_brand)
        logger.log ("-----------------------")

        #その他エリアYSS
        YSS_df_otherarea_brand = YSS_df[(YSS_df['キャンペーン'] == ('2-2_【売却／その他】一般・エリア'))| (YSS_df['キャンペーン'] == ('2-1_【売却／その他】社名・ブランド名')) | (YSS_df['キャンペーン'] == ('2-3_【売却／その他】DAS'))]
        sum_budget = YSS_df_otherarea_brand["費用"].astype(int).sum()
        sum_imp = YSS_df_otherarea_brand["表示回数"].astype(int).sum()
        sum_click = YSS_df_otherarea_brand["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global YSS_df_otherarea_brand_result
        YSS_df_otherarea_brand_result = pd.DataFrame([["Yahoo!リスティング", "YG/その他エリア", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=list(colum))
        logger.log ("その他エリアYSS")
        logger.log ("-----------------------")
        logger.log (YSS_df_otherarea_brand)
        logger.log ("-----------------------")
        
        #購入YSS
        YSS_df_buy = YSS_df[YSS_df['キャンペーン'].str.contains('購入')]
        sum_budget = YSS_df_buy["費用"].astype(int).sum()
        sum_imp = YSS_df_buy["表示回数"].astype(int).sum()
        sum_click = YSS_df_buy["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global YSS_df_buy_result
        YSS_df_buy_result = pd.DataFrame([["Yahoo!リスティング", "購入サマリ", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=list(colum))
        logger.log ("購入YSS")
        logger.log ("-----------------------")
        logger.log (YSS_df_buy)
        logger.log ("-----------------------")
        
    elif path == YSS_area_path:
        YSS_area_df = pd.read_csv(path, encoding="shift-jis", usecols=lambda x: x not in ['国/地域'])
        YSS_area_df.rename(columns={'キャンペーン名': 'キャンペーン', '市・区・郡': '地域', 'コスト': '費用', 'インプレッション数': '表示回数', '平均CPC': 'クリック単価'}, inplace=True)
        delete_row = YSS_area_df.tail(1).index
        YSS_area_df.drop(delete_row, inplace=True)
        cost = (YSS_area_df['費用'].astype(float).astype(int))
        budget = cost/0.8
        YSS_area_df.drop('費用', axis=1, inplace=True)
        YSS_area_df.insert(3, "費用", budget)
        
        #都心6区YSS
        YSS_df_six_brand = YSS_area_df[YSS_area_df['キャンペーン'].str.contains('1-2_【売却／注力】一般・エリア|1-1_【売却／注力】社名・ブランド名') & YSS_area_df['都道府県'].str.contains('東京都') & YSS_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        sum_budget = YSS_df_six_brand["費用"].astype(int).sum()
        sum_imp = YSS_df_six_brand["表示回数"].astype(int).sum()
        sum_click = YSS_df_six_brand["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global YSS_df_six_brand_result
        YSS_df_six_brand_result = pd.DataFrame([["Yahoo!リスティング", "YG/都心6区", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("都心6区YSS")
        logger.log ("-----------------------")
        logger.log (YSS_df_six_brand)
        logger.log ("-----------------------")
        
        #注力エリアYSS
        YSS_df_focus_brand = YSS_area_df[YSS_area_df['キャンペーン'].str.contains('1-2_【売却／注力】一般・エリア|1-1_【売却／注力】社名・ブランド名') & ~YSS_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区') & ~YSS_area_df['地域'].str.contains('２３区')]
        sum_budget = YSS_df_focus_brand["費用"].astype(int).sum()
        sum_imp = YSS_df_focus_brand["表示回数"].astype(int).sum()
        sum_click = YSS_df_focus_brand["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global YSS_df_focus_brand_result
        YSS_df_focus_brand_result = pd.DataFrame([["Yahoo!リスティング", "YG/注力エリア", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("注力エリアYSS")
        logger.log ("-----------------------")
        logger.log (YSS_df_focus_brand)
        logger.log ("-----------------------")

    elif path == YDN_path:
        YDN_df = pd.read_csv(path, encoding="shift-jis", usecols=lambda x: x not in ['月'])
        YDN_df.rename(columns={'キャンペーン名': 'キャンペーン', 'コスト': '費用', 'インプレッション数（旧）': '表示回数', 'クリック率（旧）': 'クリック率', '平均CPC': 'クリック単価'}, inplace=True)
        delete_row = YDN_df.tail(1).index
        YDN_df.drop(delete_row, inplace=True)
        cost = (YDN_df['費用'].astype(float).astype(int))
        budget = cost/0.8
        YDN_df.drop('費用', axis=1, inplace=True)
        YDN_df.insert(1, "費用", budget)

        #リースバックYDNリタゲ
        YDN_df_leaseback_rt = YDN_df[YDN_df['キャンペーン'].str.contains('リースバック') & YDN_df['キャンペーン'].str.contains('リターゲティング')]
        sum_budget = YDN_df_leaseback_rt["費用"].astype(int).sum()
        sum_imp = YDN_df_leaseback_rt["表示回数"].astype(int).sum()
        sum_click = YDN_df_leaseback_rt["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global YDN_df_leaseback_rt_result
        YDN_df_leaseback_rt_result = pd.DataFrame([["YG/リースバック", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=('キャンペーン', '費用', '表示回数', 'クリック数', 'クリック率', 'クリック単価'))
        logger.log ("リースバックYDNリタゲ")
        logger.log ("-----------------------")
        logger.log (YDN_df_leaseback_rt)
        logger.log ("-----------------------")

        #リースバックYDNターゲティング
        YDN_df_leaseback_tg = YDN_df[YDN_df['キャンペーン'].str.contains('リースバック') & YDN_df['キャンペーン'].str.contains('サーチターゲティング') | (YDN_df['キャンペーン'] == ('【リースバック】オーディエンスカテゴリー'))]
        global YDN_df_leaseback_tg_sum_budget
        global YDN_df_leaseback_tg_sum_imp
        global YDN_df_leaseback_tg_sum_click
        YDN_df_leaseback_tg_sum_budget = YDN_df_leaseback_tg["費用"].astype(int).sum()
        YDN_df_leaseback_tg_sum_imp = YDN_df_leaseback_tg["表示回数"].astype(int).sum()
        YDN_df_leaseback_tg_sum_click = YDN_df_leaseback_tg["クリック数"].astype(int).sum()
        logger.log ("リースバックYDNターゲット")
        logger.log ("-----------------------")
        logger.log (YDN_df_leaseback_tg)
        logger.log ("-----------------------")
        
    elif path == YDN_area_path:
        YDN_area_df = pd.read_csv(path, encoding="shift-jis", usecols=lambda x: x not in ['行政区'])
        YDN_area_df.rename(columns={'キャンペーン名': 'キャンペーン', '市区郡': '地域', 'コスト': '費用', 'インプレッション数（旧）': '表示回数', 'クリック率（旧）': 'クリック率', '平均CPC': 'クリック単価'}, inplace=True)
        delete_row = YDN_area_df.tail(1).index
        YDN_area_df.drop(delete_row, inplace=True)
        cost = (YDN_area_df['費用'].astype(float).astype(int))
        budget = cost/0.8
        YDN_area_df.drop('費用', axis=1, inplace=True)
        YDN_area_df.insert(3, "費用", budget)

        #都心6区YDNリタゲ
        YDN_df_six_rt = YDN_area_df[YDN_area_df['キャンペーン'].str.contains('【売却／注力】|【買取／注力】') & YDN_area_df['キャンペーン'].str.contains('リターゲティング|全来訪者向け') & YDN_area_df['都道府県'].str.contains('東京都') & YDN_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        sum_budget = YDN_df_six_rt["費用"].astype(int).sum()
        sum_imp = YDN_df_six_rt["表示回数"].astype(int).sum()
        sum_click = YDN_df_six_rt["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global YDN_df_six_rt_result
        YDN_df_six_rt_result = pd.DataFrame([["YDNRT", "YG/都心6区", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("都心6区YDNリタゲ")
        logger.log ("-----------------------")
        logger.log (YDN_df_six_rt)
        logger.log ("-----------------------")
        
        #都心6区YDNターゲティング
        YDN_df_six_tg = YDN_area_df[YDN_area_df['キャンペーン'].str.contains('【売却／注力】|【買取／注力】') & YDN_area_df['キャンペーン'].str.contains('サーチターゲティング') & YDN_area_df['都道府県'].str.contains('東京都') & YDN_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        global YDN_df_six_tg_sum_budget
        global YDN_df_six_tg_sum_imp
        global YDN_df_six_tg_sum_click 
        YDN_df_six_tg_sum_budget = YDN_df_six_tg["費用"].astype(int).sum()
        YDN_df_six_tg_sum_imp = YDN_df_six_tg["表示回数"].astype(int).sum()
        YDN_df_six_tg_sum_click = YDN_df_six_tg["クリック数"].astype(int).sum()
        logger.log ("都心6区YDNターゲット")
        logger.log ("-----------------------")
        logger.log (YDN_df_six_tg)
        logger.log ("-----------------------")

        #注力エリアYDNリタゲ
        YDN_df_focus_rt = YDN_area_df[YDN_area_df['キャンペーン'].str.contains('【売却／注力】|【買取／注力】') & YDN_area_df['キャンペーン'].str.contains('リターゲティング|全来訪者向け')& ~YDN_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区') & ~YDN_area_df['地域'].str.contains('--')]
        sum_budget = YDN_df_focus_rt["費用"].astype(int).sum()
        sum_imp = YDN_df_focus_rt["表示回数"].astype(int).sum()
        sum_click = YDN_df_focus_rt["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp 
        sum_cpc = sum_budget/sum_click
        global YDN_df_focus_rt_result
        YDN_df_focus_rt_result = pd.DataFrame([["YDNRT", "YG/注力エリア", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("注力エリアYDNリタゲ")
        logger.log ("-----------------------")
        logger.log (YDN_df_focus_rt)
        logger.log ("-----------------------")

        #注力エリアYDNターゲティング
        YDN_df_focus_tg = YDN_area_df[YDN_area_df['キャンペーン'].str.contains('【売却／注力】|【買取／注力】') & YDN_area_df['キャンペーン'].str.contains('サーチターゲティング') & ~YDN_area_df['地域'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区') & ~YDN_area_df['地域'].str.contains('--')]
        global YDN_df_focus_tg_sum_budget
        global YDN_df_focus_tg_sum_imp
        global YDN_df_focus_tg_sum_click
        YDN_df_focus_tg_sum_budget = YDN_df_focus_tg["費用"].astype(int).sum()
        YDN_df_focus_tg_sum_imp = YDN_df_focus_tg["表示回数"].astype(int).sum()
        YDN_df_focus_tg_sum_click = YDN_df_focus_tg["クリック数"].astype(int).sum()
        logger.log ("注力エリアYDNターゲット")
        logger.log ("-----------------------")
        logger.log (YDN_df_focus_tg)
        logger.log ("-----------------------")

        #その他エリアYDNリタゲ
        YDN_df_otherarea_rt = YDN_area_df[YDN_area_df['キャンペーン'].str.contains('【売却／通常】|【買取／通常】') & YDN_area_df['キャンペーン'].str.contains('リターゲティング|全来訪者向け') & ~YDN_area_df['地域'].str.contains('--')]
        sum_budget = YDN_df_otherarea_rt["費用"].astype(int).sum()
        sum_imp = YDN_df_otherarea_rt["表示回数"].astype(int).sum()
        sum_click = YDN_df_otherarea_rt["クリック数"].astype(int).sum()
        sum_ctr = sum_click/sum_imp
        sum_cpc = sum_budget/sum_click
        global YDN_df_otherarea_rt_result
        YDN_df_otherarea_rt_result = pd.DataFrame([["YDNRT", "YG/その他エリア", sum_budget, sum_imp, sum_click, sum_ctr, sum_cpc]], columns=(colum))
        logger.log ("その他エリアYDNリタゲ")
        logger.log ("-----------------------")
        logger.log (YDN_df_otherarea_rt)
        logger.log ("-----------------------")

        #その他エリアYDNターゲティング
        YDN_df_otherarea_tg = YDN_area_df[YDN_area_df['キャンペーン'].str.contains('【売却／通常】|【買取／通常】') & YDN_area_df['キャンペーン'].str.contains('サーチターゲティング') & ~YDN_area_df['地域'].str.contains('--')]
        global YDN_df_otherarea_tg_sum_budget
        global YDN_df_otherarea_tg_sum_imp
        global YDN_df_otherarea_tg_sum_click
        YDN_df_otherarea_tg_sum_budget = YDN_df_otherarea_tg["費用"].astype(int).sum()
        YDN_df_otherarea_tg_sum_imp = YDN_df_otherarea_tg["表示回数"].astype(int).sum()
        YDN_df_otherarea_tg_sum_click = YDN_df_otherarea_tg["クリック数"].astype(int).sum()
        logger.log ("その他エリアYDNターゲット")
        logger.log ("-----------------------")
        logger.log (YDN_df_otherarea_tg)
        logger.log ("-----------------------")
        
    elif path == webantena_path:
        weban_df = pd.read_csv(path, encoding="shift-jis")
        webantena_df = weban_df[['流入種別', '媒体', 'キャンペーン名', 'CV名', '所在地']]
        webantena_df.rename(columns={'キャンペーン名': 'キャンペーン'}, inplace=True)
        
        #都心6区リスティングコンバージョン
        GSS_six_brand = webantena_df[webantena_df['キャンペーン'].str.contains('1-2_【売却／注力】一般・エリア|1-1_【売却／注力】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Google') & webantena_df['所在地'].str.contains('東京都') & webantena_df['所在地'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        GSS_six_brand_cv = len(GSS_six_brand)
        YSS_six_brand = webantena_df[webantena_df['キャンペーン'].str.contains('1-2_【売却／注力】一般・エリア|1-1_【売却／注力】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Yahoo!') & webantena_df['所在地'].str.contains('東京都') & webantena_df['所在地'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        YSS_six_brand_cv = len(YSS_six_brand)
        global six_brand_cv
        six_brand_cv = GSS_six_brand_cv + YSS_six_brand_cv
        logger.log ("都心6区リスティングコンバージョン")
        logger.log ("-----------------------")
        logger.log (GSS_six_brand)
        logger.log (YSS_six_brand)
        logger.log ("-----------------------")

        #都心6区リスティング社名ブランド名のコンバージョン
        GSS_six_brandonly = webantena_df[webantena_df['キャンペーン'].str.contains('1-1_【売却／注力】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Google') & webantena_df['所在地'].str.contains('東京都') & webantena_df['所在地'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        GSS_six_brandonly_cv = len(GSS_six_brandonly)
        YSS_six_brandonly = webantena_df[webantena_df['キャンペーン'].str.contains('1-1_【売却／注力】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Yahoo!') & webantena_df['所在地'].str.contains('東京都') & webantena_df['所在地'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        YSS_six_brandonly_cv = len(YSS_six_brandonly)
        global six_brandonly_cv
        six_brandonly_cv = GSS_six_brandonly_cv + YSS_six_brandonly_cv
        logger.log ("都心6区リスティング社名ブランド名のコンバージョン")
        logger.log ("-----------------------")
        logger.log (GSS_six_brandonly)
        logger.log (YSS_six_brandonly)
        logger.log ("-----------------------")
        
        #都心6区ディスプレイRMRTコンバージョン
        GDN_six = webantena_df[(webantena_df['媒体'] == ('Google')) & (webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('【売却／注力／GDN】') & webantena_df['キャンペーン'].str.contains('リマーケティング|全来訪者向け') & ~webantena_df['CV名'].str.contains('買いたい') & webantena_df['所在地'].str.contains('東京都') & webantena_df['所在地'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        GDN_six_cv = len(GDN_six)
        YDN_six = webantena_df[webantena_df['媒体'].str.contains('Yahoo!') & (webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('【売却／注力】|【買取／注力】') & webantena_df['キャンペーン'].str.contains('リターゲティング|全来訪者向け') & ~webantena_df['CV名'].str.contains('買いたい') & webantena_df['所在地'].str.contains('東京都') & webantena_df['所在地'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        YDN_six_cv = len(YDN_six)
        global six_dn_cv
        six_dn_cv = GDN_six_cv + YDN_six_cv
        logger.log ("都心6区ディスプレイRMRTコンバージョン")
        logger.log ("-----------------------")
        logger.log (GDN_six)
        logger.log (YDN_six)
        logger.log ("-----------------------")

        #都心6区ディスプレイTGコンバージョン
        YDN_six_tg = webantena_df[(webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('【売却／注力】|【買取／注力】') & ~webantena_df['CV名'].str.contains('買いたい') & webantena_df['キャンペーン'].str.contains('サーチターゲティング') & webantena_df['所在地'].str.contains('東京都') & webantena_df['所在地'].str.contains('千代田区|中央区|港区|新宿区|渋谷区|文京区')]
        global YDN_six_tg_cv
        YDN_six_tg_cv = len(YDN_six)
        logger.log ("都心6区ディスプレイTGコンバージョン")
        logger.log ("-----------------------")
        logger.log (YDN_six_tg)
        logger.log ("-----------------------")
        
        #注力エリアリスティングコンバージョン
        GSS_focus_brand = webantena_df[webantena_df['キャンペーン'].str.contains('1-2_【売却／注力】一般・エリア|1-1_【売却／注力】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Google') & ~webantena_df['所在地'].str.contains('東京都千代田区|東京都中央区|東京都港区|東京都新宿区|東京都渋谷区|東京都文京区', na=False)]
        GSS_focus_brand_cv = len(GSS_focus_brand)
        YSS_focus_brand = webantena_df[webantena_df['キャンペーン'].str.contains('1-2_【売却／注力】一般・エリア|1-1_【売却／注力】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Yahoo!') & ~webantena_df['所在地'].str.contains('東京都千代田区|東京都中央区|東京都港区|東京都新宿区|東京都渋谷区|東京都文京区', na=False)]
        YSS_focus_brand_cv = len(YSS_focus_brand)
        global focus_brand_cv
        focus_brand_cv = GSS_focus_brand_cv + YSS_focus_brand_cv
        logger.log ("注力エリアリスティングコンバージョン")
        logger.log ("-----------------------")
        logger.log (GSS_focus_brand)
        logger.log (YSS_focus_brand)
        logger.log ("-----------------------")

        #注力エリアリスティング社名ブランド名のコンバージョン
        GSS_focus_brandonly = webantena_df[webantena_df['キャンペーン'].str.contains('1-1_【売却／注力】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Google') & ~webantena_df['所在地'].str.contains('東京都千代田区|東京都中央区|東京都港区|東京都新宿区|東京都渋谷区|東京都文京区', na=False)]
        GSS_focus_brandonly_cv = len(GSS_focus_brandonly)
        YSS_focus_brandonly = webantena_df[webantena_df['キャンペーン'].str.contains('1-1_【売却／注力】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Yahoo!') & ~webantena_df['所在地'].str.contains('東京都千代田区|東京都中央区|東京都港区|東京都新宿区|東京都渋谷区|東京都文京区', na=False)]
        YSS_focus_brandonly_cv = len(YSS_focus_brandonly)
        global focus_brandonly_cv
        focus_brandonly_cv = GSS_focus_brandonly_cv + YSS_focus_brandonly_cv
        logger.log ("注力エリアリスティング社名ブランド名のコンバージョン")
        logger.log ("-----------------------")
        logger.log (GSS_focus_brandonly)
        logger.log (YSS_focus_brandonly)
        logger.log ("-----------------------")
        
        #注力エリアディスプレイRMRTコンバージョン
        GDN_focus = webantena_df[(webantena_df['媒体'] == ('Google')) & (webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('【売却／注力／GDN】') & webantena_df['キャンペーン'].str.contains('リマーケティング|全来訪者向け') & ~webantena_df['CV名'].str.contains('買いたい') & ~webantena_df['所在地'].str.contains('東京都千代田区|東京都中央区|東京都港区|東京都新宿区|東京都渋谷区|東京都文京区', na=False)]
        GDN_focus_cv = len(GDN_focus)
        YDN_focus = webantena_df[webantena_df['媒体'].str.contains('Yahoo!') & (webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('【売却／注力】|【買取／注力】') & webantena_df['キャンペーン'].str.contains('リターゲティング|全来訪者向け') & ~webantena_df['CV名'].str.contains('買いたい') & ~webantena_df['所在地'].str.contains('東京都千代田区|東京都中央区|東京都港区|東京都新宿区|東京都渋谷区|東京都文京区', na=False)]
        YDN_focus_cv = len(YDN_focus)
        global focus_dn_cv
        focus_dn_cv = GDN_focus_cv + YDN_focus_cv
        logger.log ("注力エリアディスプレイRMRTコンバージョン")
        logger.log ("-----------------------")
        logger.log (GDN_focus)
        logger.log (YDN_focus)
        logger.log ("-----------------------")

        #注力エリアディスプレイTGコンバージョン
        YDN_focus_tg = webantena_df[(webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('【売却／注力】|【買取／注力】') & webantena_df['キャンペーン'].str.contains('サーチターゲティング') & ~webantena_df['CV名'].str.contains('買いたい') & ~webantena_df['所在地'].str.contains('東京都千代田区|東京都中央区|東京都港区|東京都新宿区|東京都渋谷区|東京都文京区', na=False)]
        global YDN_focus_tg_cv
        YDN_focus_tg_cv = len(YDN_focus_tg)
        logger.log ("注力エリアディスプレイTGコンバージョン")
        logger.log ("-----------------------")
        logger.log (YDN_focus_tg)
        logger.log ("-----------------------")
        
        #その他エリアリスティングコンバージョン
        GSS_otherarea_brand = webantena_df[webantena_df['キャンペーン'].str.contains('2-2_【売却／その他】一般・エリア|2-1_【売却／その他】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Google')]
        GSS_otherarea_brand_cv = len(GSS_otherarea_brand)
        YSS_otherarea_brand = webantena_df[webantena_df['キャンペーン'].str.contains('2-2_【売却／その他】一般・エリア|2-1_【売却／その他】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Yahoo!')]
        YSS_otherarea_brand_cv = len(YSS_otherarea_brand)
        global otherarea_brand_cv
        otherarea_brand_cv = GSS_otherarea_brand_cv + YSS_otherarea_brand_cv
        logger.log ("その他エリアリスティングコンバージョン")
        logger.log ("-----------------------")
        logger.log (GSS_otherarea_brand)
        logger.log (YSS_otherarea_brand)
        logger.log ("-----------------------")
        
        #その他エリアリスティング社名ブランド名のコンバージョン
        GSS_otherarea_brandonly = webantena_df[webantena_df['キャンペーン'].str.contains('2-1_【売却／その他】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Google')]
        GSS_otherarea_brandonly_cv = len(GSS_otherarea_brandonly)
        YSS_otherarea_brandonly = webantena_df[webantena_df['キャンペーン'].str.contains('2-1_【売却／その他】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Yahoo!')]
        YSS_otherarea_brandonly_cv = len(YSS_otherarea_brandonly)
        global otherarea_brandonly_cv
        otherarea_brandonly_cv = GSS_otherarea_brandonly_cv + YSS_otherarea_brandonly_cv
        logger.log ("その他エリアリスティング社名ブランド名のコンバージョン")
        logger.log ("-----------------------")
        logger.log (GSS_otherarea_brandonly)
        logger.log (YSS_otherarea_brandonly)
        logger.log ("-----------------------")
        
        #その他エリアディスプレイRMRTコンバージョン
        GDN_otherarea = webantena_df[(webantena_df['媒体'] == ('Google')) & (webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('【売却／通常／GDN】') & webantena_df['キャンペーン'].str.contains('リマーケティング|全来訪者向け') & ~webantena_df['CV名'].str.contains('買いたい')]
        GDN_otherarea_cv = len(GDN_otherarea)
        YDN_otherarea = webantena_df[webantena_df['媒体'].str.contains('Yahoo!') & (webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('【売却／通常】|【買取／通常】') & webantena_df['キャンペーン'].str.contains('リターゲティング|全来訪者向け') & ~webantena_df['CV名'].str.contains('買いたい')]
        YDN_otherarea_cv = len(YDN_otherarea)
        global otherarea_dn_cv
        otherarea_dn_cv = GDN_otherarea_cv + YDN_otherarea_cv
        logger.log ("その他エリアディスプレイRMRTコンバージョン")
        logger.log ("-----------------------")
        logger.log (GDN_otherarea)
        logger.log (YDN_otherarea)
        logger.log ("-----------------------")

        #その他エリアディスプレイTGコンバージョン
        YDN_otherarea_tg = webantena_df[(webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('【売却／通常】|【買取／通常】') & webantena_df['キャンペーン'].str.contains('サーチターゲティング') & ~webantena_df['CV名'].str.contains('買いたい')]
        global YDN_otherarea_tg_cv
        YDN_otherarea_tg_cv = len(YDN_otherarea_tg)
        logger.log ("その他エリアディスプレイTGコンバージョン")
        logger.log ("-----------------------")
        logger.log (YDN_otherarea_tg)
        logger.log ("-----------------------")

        #リースバックリスティングコンバージョン
        GSS_leaseback_brand = webantena_df[webantena_df['キャンペーン'].str.contains('3-2_【リースバック】一般・エリア|3-3_【リースバック】単体ワード|3-1_【リースバック】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Google')]
        GSS_leaseback_brand_cv = len(GSS_leaseback_brand)
        YSS_leaseback_brand = webantena_df[webantena_df['キャンペーン'].str.contains('3-2_【リースバック】一般・エリア|3-3_【リースバック】単体ワード|3-1_【リースバック】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Yahoo!')]
        YSS_leaseback_brand_cv = len(YSS_leaseback_brand)
        global leaseback_brand_cv
        leaseback_brand_cv = GSS_leaseback_brand_cv + YSS_leaseback_brand_cv
        logger.log ("リースバックリスティングコンバージョン")
        logger.log ("-----------------------")
        logger.log (GSS_leaseback_brand)
        logger.log (YSS_leaseback_brand)
        logger.log ("-----------------------")
        
        #リースバックリスティング社名ブランド名のコンバージョン
        GSS_leaseback_brandonly = webantena_df[webantena_df['キャンペーン'].str.contains('3-1_【リースバック】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Google')]
        GSS_leaseback_brandonly_cv = len(GSS_leaseback_brandonly)
        YSS_leaseback_brandonly = webantena_df[webantena_df['キャンペーン'].str.contains('3-1_【リースバック】社名・ブランド名') & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Yahoo!')]
        YSS_leaseback_brandonly_cv = len(YSS_leaseback_brandonly)
        global leaseback_brandonly_cv
        leaseback_brandonly_cv = GSS_leaseback_brandonly_cv + YSS_leaseback_brandonly_cv
        logger.log ("リースバックリスティング社名ブランド名のコンバージョン")
        logger.log ("-----------------------")
        logger.log (GSS_leaseback_brandonly)
        logger.log (YSS_leaseback_brandonly)
        logger.log ("-----------------------")
        
        #リースバックディスプレイRMRTコンバージョン
        GDN_leaseback = webantena_df[(webantena_df['媒体'] == ('Google')) & (webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('リースバック') & webantena_df['キャンペーン'].str.contains('リマーケティング') & ~webantena_df['CV名'].str.contains('買いたい')]
        GDN_leaseback_cv = len(GDN_leaseback)
        YDN_leaseback = webantena_df[webantena_df['媒体'].str.contains('Yahoo!') & (webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('リースバック') & webantena_df['キャンペーン'].str.contains('リターゲティング') & ~webantena_df['CV名'].str.contains('買いたい')]
        YDN_leaseback_cv = len(YDN_leaseback)
        global leaseback_dn_cv
        leaseback_dn_cv = GDN_leaseback_cv + YDN_leaseback_cv
        logger.log ("リースバックディスプレイRMRTコンバージョン")
        logger.log ("-----------------------")
        logger.log (GDN_leaseback)
        logger.log (YDN_leaseback)
        logger.log ("-----------------------")

        #リースバックディスプレイTGコンバージョン
        YDN_leaseback_tg = webantena_df[(webantena_df['流入種別'] == ('バナー')) & webantena_df['キャンペーン'].str.contains('リースバック') & webantena_df['キャンペーン'].str.contains('サーチターゲティング') & ~webantena_df['CV名'].str.contains('買いたい')]
        global YDN_leaseback_tg_cv
        YDN_leaseback_tg_cv = len(YDN_leaseback_tg)
        logger.log ("リースバックディスプレイTGコンバージョン")
        logger.log ("-----------------------")
        logger.log (YDN_leaseback_tg)
        logger.log ("-----------------------")
        
        #社名・ブランド名リスティングコンバージョン
        GSS_brand = webantena_df[webantena_df['キャンペーン'].str.contains('社名・ブランド名') & ~webantena_df['キャンペーン'].str.contains('購入' ,na=False) & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Google')]
        GSS_brand_cv = len(GSS_brand)
        YSS_brand = webantena_df[webantena_df['キャンペーン'].str.contains('社名・ブランド名') & ~webantena_df['キャンペーン'].str.contains('購入' ,na=False) & ~webantena_df['CV名'].str.contains('買いたい') & (webantena_df['流入種別'] == ('リスティング')) & webantena_df['媒体'].str.contains('Yahoo!')]
        YSS_brand_cv = len(YSS_brand)
        global brand_cv
        brand_cv = GSS_brand_cv + YSS_brand_cv
        logger.log ("社名・ブランド名リスティングコンバージョン")
        logger.log ("-----------------------")
        logger.log (GSS_brand)
        logger.log (YSS_brand)
        logger.log ("-----------------------")
        
        #クリテオコンバージョン
        criteo_sell = webantena_df[(webantena_df['媒体'] == ('Criteo')) & webantena_df['CV名'].str.contains('無料売却査定|全体お問い合わせ|マンション買い取り_お問い合わせ')]
        criteo_leaseback = webantena_df[(webantena_df['媒体'] == ('Criteo')) & webantena_df['CV名'].str.contains('リースバック')]
        criteo_buy = webantena_df[(webantena_df['媒体'] == ('Criteo')) & ~webantena_df['CV名'].str.contains('無料売却査定|全体お問い合わせ|マンション買い取り_お問い合わせ|リースバック')]
        
        global criteo_sell_cv
        global criteo_leaseback_cv
        global criteo_buy_cv
        global criteo_cv
        criteo_sell_cv = len(criteo_sell)
        criteo_leaseback_cv = len(criteo_leaseback)
        criteo_buy_cv = len(criteo_buy)
        criteo_cv = criteo_sell_cv + criteo_leaseback_cv + criteo_buy_cv
        logger.log ("クリテオコンバージョン")
        logger.log ("-----------------------")
        logger.log (criteo_sell)
        logger.log (criteo_leaseback)
        logger.log (criteo_buy)
        logger.log ("-----------------------")

        #Facebookコンバージョン
        facebook_sell = webantena_df[(webantena_df['媒体'] == ('Facebook')) & webantena_df['CV名'].str.contains('無料売却査定|全体お問い合わせ|マンション買い取り_お問い合わせ')]
        facebook_leaseback = webantena_df[(webantena_df['媒体'] == ('Facebook')) & webantena_df['CV名'].str.contains('リースバック')]
        facebook_buy = webantena_df[(webantena_df['媒体'] == ('Facebook')) & ~webantena_df['CV名'].str.contains('無料売却査定|全体お問い合わせ|マンション買い取り_お問い合わせ|リースバック')]
        global facebook_sell_cv
        global facebook_leaseback_cv
        global facebook_buy_cv
        global facebook_cv
        facebook_sell_cv = len(facebook_sell)
        facebook_leaseback_cv = len(facebook_leaseback)
        facebook_buy_cv = len(facebook_buy)
        facebook_cv = facebook_sell_cv + facebook_leaseback_cv + facebook_buy_cv
        logger.log ("Facebookコンバージョン")
        logger.log ("-----------------------")
        logger.log (facebook_sell)
        logger.log (facebook_leaseback)
        logger.log (facebook_buy)
        logger.log ("-----------------------")

        #Smartnewsコンバージョン
        smartnews_sell = webantena_df[(webantena_df['媒体'] == ('SmartNews')) & webantena_df['CV名'].str.contains('無料売却査定|全体お問い合わせ|マンション買い取り_お問い合わせ')]
        smartnews_leaseback = webantena_df[(webantena_df['媒体'] == ('SmartNews')) & webantena_df['CV名'].str.contains('リースバック')]
        smartnews_buy = webantena_df[(webantena_df['媒体'] == ('SmartNews')) & ~webantena_df['CV名'].str.contains('無料売却査定|全体お問い合わせ|マンション買い取り_お問い合わせ|リースバック')]
        global smartnews_sell_cv
        global smartnews_leaseback_cv
        global smartnews_buy_cv
        global smartnews_cv
        smartnews_sell_cv = len(smartnews_sell)
        smartnews_leaseback_cv = len(smartnews_leaseback)
        smartnews_buy_cv = len(smartnews_buy)
        smartnews_cv = smartnews_sell_cv + smartnews_leaseback_cv + smartnews_buy_cv
        logger.log ("Smartnewsコンバージョン")
        logger.log ("-----------------------")
        logger.log (smartnews_sell)
        logger.log (smartnews_leaseback)
        logger.log (smartnews_buy)
        logger.log ("-----------------------")

        #購入コンバージョン
        buy = webantena_df[webantena_df['流入種別'].str.contains('リスティング|バナー') & webantena_df['キャンペーン'].str.contains('購入') & ~webantena_df['キャンペーン'].str.contains('掲載')]
        global buy_cv
        buy_cv = len(buy)
        logger.log ("購入コンバージョン")
        logger.log ("-----------------------")
        logger.log (buy)
        logger.log ("-----------------------")
        
        #売却流入の買いたいコンバージョン
        buywant = webantena_df[webantena_df['流入種別'].str.contains('リスティング|バナー') & webantena_df['媒体'].str.contains('Google|Yahoo') & ~webantena_df['キャンペーン'].str.contains('掲載|購入|SP') & webantena_df['CV名'].str.contains('買いたい')]
        global buywant_cv
        buywant_cv = len(buywant)
        logger.log ("売却流入からの買いたいコンバージョン")
        logger.log ("-----------------------")
        logger.log (buywant)
        logger.log ("-----------------------")

    elif path == criteo_path:
        global criteo_df
        global criteo_imp
        global criteo_click
        global criteo_budget
        criteo_df = pd.read_csv(path, encoding="shift-jis", skiprows=1, usecols=lambda x: x not in ['広告主 ID', '広告主名', 'キャンペーン ID', 'キャンペーン名', '通貨', 'Revenue', 'Sales', 'COS', 'CPO', 'CVR'])
        criteo_df.rename(columns={'コスト': '費用', 'インプレッション': '表示回数', 'Clicks': 'クリック数', 'CTR': 'クリック率',}, inplace=True)
        criteo_df.dropna(how='all', inplace=True)
        delete_row = criteo_df.tail(1).index
        criteo_df.drop(delete_row, inplace=True)
        criteo_imp = criteo_df['表示回数'].str.replace(',', '').astype(float).astype(int)
        cost = (criteo_df['費用'].str.replace(chr(92), '').str.replace(',', '').astype(float).astype(int))
        criteo_budget = cost*1.43
        criteo_click = criteo_df['クリック数'].str.replace(',', '').astype(float).astype(int)
        criteo_ctr = criteo_df['クリック率'].str.replace('%', '').astype(float)
        criteo_ctr = criteo_ctr/50
        criteo_cpc = criteo_budget/criteo_click
        criteo_cvr = criteo_cv/criteo_click
        criteo_cpa = criteo_budget/criteo_cv
        criteo_budget = int(criteo_budget)
        criteo_cpa = criteo_cpa.astype(float)
        criteo_df.drop('費用', axis=1, inplace=True)
        criteo_df.insert(0, "費用", criteo_budget)
        criteo_df.drop('表示回数', axis=1, inplace=True)
        criteo_df.insert(1, "表示回数", criteo_imp)
        criteo_df.drop('クリック数', axis=1, inplace=True)
        criteo_df.insert(2, "クリック数", criteo_click)
        criteo_df.drop('クリック率', axis=1, inplace=True)
        criteo_df.insert(3, "クリック率", criteo_ctr)
        criteo_df.insert(4, "クリック単価", criteo_cpc)
        criteo_df.insert(5, "コンバージョン数", criteo_cv)
        criteo_df.insert(6, "コンバージョン率", criteo_cvr)
        criteo_df.insert(7, "コンバージョン単価", criteo_cpa)
        criteo_df.insert(8, "売却CV", criteo_sell_cv)
        criteo_df.insert(9, "リースバックCV", criteo_leaseback_cv)
        criteo_df.insert(10, "購入CV", criteo_buy_cv)
        criteo_df = criteo_df.replace(np.inf,np.nan).fillna(0)
    
    elif path == facebook_path:
        global facebook_df_result
        global facebook_sum_budget
        facebook_df = pd.read_csv(path, usecols=lambda x: x in ['キャンペーン名', 'インプレッション', '消化金額 (JPY)', 'リンククリック(ユニーク)'])
        facebook_df.rename(columns={'キャンペーン名': 'キャンペーン', 'インプレッション': '表示回数', 'リンククリック(ユニーク)': 'クリック数', '消化金額 (JPY)': '費用'}, inplace=True)
        cost = facebook_df['費用'].astype(float).astype(int)
        budget = cost/0.8
        facebook_df.drop('費用', axis=1, inplace=True)
        facebook_df.insert(1, "費用", budget)
        facebook_sum_budget = facebook_df["費用"].sum()
        facebook_sum_imp = facebook_df["表示回数"].sum()
        facebook_sum_click = facebook_df["クリック数"].sum()
        facebook_sum_ctr = facebook_sum_click/facebook_sum_imp 
        facebook_sum_cpc = facebook_sum_budget/facebook_sum_click
        facebook_cvr = facebook_cv/facebook_sum_click 
        facebook_cpa = facebook_sum_budget/facebook_cv
        facebook_df_result = pd.DataFrame([[facebook_sum_budget, facebook_sum_imp, facebook_sum_click, facebook_sum_ctr, facebook_sum_cpc, facebook_cv, facebook_cvr, facebook_cpa, facebook_sell_cv, facebook_leaseback_cv, facebook_buy_cv]], columns=(colum_result_other))
        facebook_df_result = facebook_df_result.replace(np.inf,np.nan).fillna(0)
        logger.log ("facebook_raw")
        logger.log ("-----------------------")
        logger.log (facebook_df)
        logger.log ("-----------------------")

    elif path == smartnews_path:
        global smartnews_df_result
        global smartnews_sum_budget
        smartnews_df = pd.read_csv(path, encoding="shift-jis")
        smartnews_df.rename(columns={'Impressions': '表示回数', 'Clicks': 'クリック数', 'CTR': 'クリック率', 'CTR': 'クリック率', 'CPC': 'クリック単価', 'CPC': 'クリック単価', 'ご利用金額': '費用'}, inplace=True)
        cost = smartnews_df["費用"].str.replace(chr(92), '').str.replace(',', '').astype(float).astype(int)
        budget = cost/0.8
        smartnews_df.drop('費用', axis=1, inplace=True)
        smartnews_df.insert(1, "費用", budget)
        smartnews_sum_budget = smartnews_df["費用"].sum()
        smartnews_imp = smartnews_df["表示回数"].str.replace(',', '').astype("int64")
        smartnews_sum_imp = smartnews_imp.sum()
        if smartnews_df["クリック数"].dtypes == "object":
            smartnews_click = smartnews_df["クリック数"].str.replace(',', '').astype("int64")
        else:
            pass
        smartnews_sum_click = smartnews_df["クリック数"].sum()
        smartnews_sum_ctr = smartnews_sum_click/smartnews_sum_imp
        smartnews_sum_cpc = smartnews_sum_budget/smartnews_sum_click
        smartnews_cvr = smartnews_cv/smartnews_sum_click 
        smartnews_cpa = smartnews_sum_budget/smartnews_cv
        smartnews_df_result = pd.DataFrame([[smartnews_sum_budget, smartnews_sum_imp, smartnews_sum_click, smartnews_sum_ctr, smartnews_sum_cpc, smartnews_cv, smartnews_cvr, smartnews_cpa, smartnews_sell_cv, smartnews_leaseback_cv, smartnews_buy_cv]], columns=(colum_result_other))
        smartnews_df_result = smartnews_df_result.replace(np.inf,np.nan).fillna(0)
        logger.log ("smartnews_raw")
        logger.log ("-----------------------")
        logger.log (smartnews_df)
        logger.log ("-----------------------")
        
#----------スプレッドシートの「全体サマリ」「購入サマリ」シートにデータを入れ込む関数----------
def add_data_all_buy(selectworksheet, get_data):
    cell = selectworksheet.find(str(end_date_month) + '月計')
    month_cell = cell.row
    selectworksheet.insert_row([""], index=month_cell)
    weeknum = "=WEEKNUM(B" + str(month_cell) + ",2)-WEEKNUM(EOMONTH(B" + str(month_cell) + ",-1)+1,2)+1"'&"週目' + str(end_date_month) + '月"'
    selectworksheet.update_acell("A" + str(month_cell), weeknum)
    selectworksheet.update_acell("B" + str(month_cell), end_date)
    if selectworksheet == all_worksheet:
        doing = "=D" + str(month_cell) +"/'目標'!B$" + str(end_date_month+1)
    elif selectworksheet == buy_worksheet:
        doing = "=E" + str(month_cell) +"-'目標'!Q$" + str(end_date_month+1)
        selectworksheet.update_acell("I" + str(month_cell), buywant_cv) 
    selectworksheet.update_acell("F" + str(month_cell), doing)
    all_cell_list = selectworksheet.range("C" + str(month_cell) + ":" + "E" + str(month_cell))
    for all_cell in all_cell_list:
        all_val = get_data.loc[0][all_cell.col-3]
        all_cell.value = all_val
    selectworksheet.update_cells(all_cell_list)
    sum_row = month_cell + 1
    sum_target = selectworksheet.find("1週目"+ str(end_date_month) + "月")
    sum_target_row = sum_target.row
    summonth_budget = "=SUM(C" + str(sum_target_row) + ":" + "C" + str(month_cell) + ")"
    summonth_cv = "=SUM(D" + str(sum_target_row) + ":" + "D" + str(month_cell) + ")"
    summonth_cpa = "=C" + str(sum_row) + "/" + "D" + str(sum_row)
    if selectworksheet == all_worksheet:
        summonth_doing = "=D" + str(sum_row) +"/'目標'!B$" + str(end_date_month+1)
    elif selectworksheet == buy_worksheet:
        expect_cv = "=D" + str(sum_row) + "/" + str(end_date_date) + "*" + str(end_lastday)
        expect_cv_dif = "='目標'!P$" + str(end_date_month+1) + "-" + "G" + str(sum_row)    
        summonth_doing = "=E" + str(sum_row) +"-'目標'!Q$" + str(end_date_month+1)
        selectworksheet.update_acell("G" + str(sum_row), expect_cv)
        selectworksheet.update_acell("H" + str(sum_row), expect_cv_dif)
    selectworksheet.update_acell("C" + str(sum_row), summonth_budget)
    selectworksheet.update_acell("D" + str(sum_row), summonth_cv)
    selectworksheet.update_acell("E" + str(sum_row), summonth_cpa)
    selectworksheet.update_acell("F" + str(sum_row), summonth_doing)

#----------スプレッドシートの「全体サマリ」「購入サマリ」以外のシートにデータを入れ込む関数----------
def add_data_fix(selectworksheet, select_table_row, get_data, sheetname):
    selectworksheet.insert_row([""], index=select_table_row)
    #A列とB列に周期と期間を記載
    weeknum = "=WEEKNUM(B" + str(select_table_row) + ",2)-WEEKNUM(EOMONTH(B" + str(select_table_row) + ",-1)+1,2)+1"'&"週目' + str(end_date_month) + '月' + sheetname + '"' 
    selectworksheet.update_acell("A" + str(select_table_row), weeknum)
    selectworksheet.update_acell("B" + str(select_table_row), end_date)
    #YG全体サマリシートに各種別の行を取得し、進捗率を入力。数値を記載する範囲を指定
    if selectworksheet == YG_all_worksheet:
        if select_table_row == YG_all_table_row:
            doing = "=K" + str(select_table_row) +"-'目標'!D$" + str(end_date_month+1)
        elif select_table_row == six_all_table_row:
            doing = "=K" + str(select_table_row) +"-'目標'!F$" + str(end_date_month+1)
        elif select_table_row == focus_all_table_row:
            doing = "=K" + str(select_table_row) +"-'目標'!H$" + str(end_date_month+1)
        elif select_table_row == otherarea_all_table_row:
            doing = "=K" + str(select_table_row) +"-'目標'!J$" + str(end_date_month+1)
        elif select_table_row == leaseback_all_table_row:
            doing = "=K" + str(select_table_row) +"-'目標'!L$" + str(end_date_month+1)    
        selectworksheet.update_acell("M" + str(select_table_row), doing)
        YG_all_cell_list = selectworksheet.range("D" + str(select_table_row) + ":" + "L" + str(select_table_row))
    #都心6区、注力、その他エリア、リースバック、購入、社名ブランドシートに数値を記載する範囲を指定
    elif selectworksheet == six_worksheet or selectworksheet == focus_worksheet or selectworksheet == otherarea_worksheet or selectworksheet == leaseback_worksheet or selectworksheet == brand_worksheet:
        YG_all_cell_list = selectworksheet.range("D" + str(select_table_row) + ":" + "K" + str(select_table_row))
    #Criteoシートに進捗率を入力。数値を記載する範囲を指定
    elif selectworksheet == criteo_worksheet:
        doing = "=K" + str(select_table_row) +"-'目標'!O$" + str(end_date_month+1)
        selectworksheet.update_acell("O" + str(select_table_row), doing)
        YG_all_cell_list = selectworksheet.range("D" + str(select_table_row) + ":" + "N" + str(select_table_row))
    #Facebookシートに進捗率を入力。数値を記載する範囲を指定
    elif selectworksheet == facebook_worksheet:
        doing = "=K" + str(select_table_row) +"-'目標'!S$" + str(end_date_month+1)
        selectworksheet.update_acell("O" + str(select_table_row), doing)
        YG_all_cell_list = selectworksheet.range("D" + str(select_table_row) + ":" + "N" + str(select_table_row))
    #Smartnewsシートに進捗率を入力。数値を記載する範囲を指定
    elif selectworksheet == smartnews_worksheet:
        doing = "=K" + str(select_table_row) +"-'目標'!U$" + str(end_date_month+1)    
        selectworksheet.update_acell("O" + str(select_table_row), doing)
        YG_all_cell_list = selectworksheet.range("D" + str(select_table_row) + ":" + "N" + str(select_table_row))
    
    #for文で数値を一個ずつ記載する
    for YG_all_cell in YG_all_cell_list:
        YG_val = get_data.loc[0][YG_all_cell.col-4]
        YG_all_cell.value = YG_val
    selectworksheet.update_cells(YG_all_cell_list)

    #合計値を設定し、入力する
    ##1週目〇月を基準としてSUMをするため、行を特定し取得する
    fix_sum_row = select_table_row + 1
    fix_sum_target = selectworksheet.find("1週目"+ str(end_date_month) + "月" + sheetname)
    fix_sum_target_row = fix_sum_target.row
    #それぞれ適した列に関数を設定
    fix_summonth_budget = "=SUM(D" + str(fix_sum_target_row) + ":" + "D" + str(select_table_row) + ")"
    fix_summonth_imp = "=SUM(E" + str(fix_sum_target_row) + ":" + "E" + str(select_table_row) + ")"
    fix_summonth_click = "=SUM(F" + str(fix_sum_target_row) + ":" + "F" + str(select_table_row) + ")"
    fix_summonth_ctr = "=F" + str(fix_sum_row) + "/" + "E" + str(fix_sum_row)
    fix_summonth_cpc = "=D" + str(fix_sum_row) + "/" + "F" + str(fix_sum_row)
    fix_summonth_cv = "=SUM(I" + str(fix_sum_target_row) + ":" + "I" + str(select_table_row) + ")"
    fix_summonth_cvr = "=I" + str(fix_sum_row) + "/" + "F" + str(fix_sum_row)
    fix_summonth_cpa = "=D" + str(fix_sum_row) + "/" + "I" + str(fix_sum_row)
    #YG全体サマリシートのSUM設定
    if selectworksheet == YG_all_worksheet:
        if select_table_row == YG_all_table_row:
            expect_cv = "=I" + str(fix_sum_row) + "/" + str(end_date_date) + "*" + str(end_lastday)
            expect_cv_dif = "='目標'!C$" + str(end_date_month+1) + "-" + "N" + str(fix_sum_row)
            fix_summonth_doing = "=K" + str(fix_sum_row) +"-'目標'!D$" + str(end_date_month+1)
        elif select_table_row == six_all_table_row:
            expect_cv = "=I" + str(fix_sum_row) + "/" + str(end_date_date) + "*" + str(end_lastday)
            expect_cv_dif = "='目標'!E$" + str(end_date_month+1) + "-" + "N" + str(fix_sum_row)
            fix_summonth_doing = "=K" + str(fix_sum_row) +"-'目標'!F$" + str(end_date_month+1)
        elif select_table_row == focus_all_table_row:
            expect_cv = "=I" + str(fix_sum_row) + "/" + str(end_date_date) + "*" + str(end_lastday)
            expect_cv_dif = "='目標'!G$" + str(end_date_month+1) + "-" + "N" + str(fix_sum_row)
            fix_summonth_doing = "=K" + str(fix_sum_row) +"-'目標'!H$" + str(end_date_month+1)
        elif select_table_row == otherarea_all_table_row:
            expect_cv = "=I" + str(fix_sum_row) + "/" + str(end_date_date) + "*" + str(end_lastday)
            expect_cv_dif = "='目標'!I$" + str(end_date_month+1) + "-" + "N" + str(fix_sum_row)
            fix_summonth_doing = "=K" + str(fix_sum_row) +"-'目標'!J$" + str(end_date_month+1)
        elif select_table_row == leaseback_all_table_row:
            expect_cv = "=I" + str(fix_sum_row) + "/" + str(end_date_date) + "*" + str(end_lastday)
            expect_cv_dif = "='目標'!K$" + str(end_date_month+1) + "-" + "N" + str(fix_sum_row)
            fix_summonth_doing = "=K" + str(fix_sum_row) +"-'目標'!L$" + str(end_date_month+1)
        fix_summonth_brandcv = "=SUM(L" + str(fix_sum_target_row) + ":" + "L" + str(select_table_row) + ")"
        selectworksheet.update_acell("L" + str(fix_sum_row), fix_summonth_brandcv)
        selectworksheet.update_acell("M" + str(fix_sum_row), fix_summonth_doing)
        selectworksheet.update_acell("N" + str(fix_sum_row), expect_cv)
        selectworksheet.update_acell("O" + str(fix_sum_row), expect_cv_dif)
    #都心6区、注力、その他エリア、リースバック、購入、社名ブランドシートの場合はスルー
    elif selectworksheet == six_worksheet or selectworksheet == focus_worksheet or selectworksheet == otherarea_worksheet or selectworksheet == leaseback_worksheet or selectworksheet == brand_worksheet:
        pass
    #CriteoシートのSUM設定
    elif selectworksheet == criteo_worksheet:
        fix_summonth_doing = "=K" + str(fix_sum_row) +"-'目標'!O$" + str(end_date_month+1)
        fix_summonth_cv_sell = "=SUM(L" + str(fix_sum_target_row) + ":" + "L" + str(select_table_row) + ")"
        fix_summonth_cv_leaseback = "=SUM(M" + str(fix_sum_target_row) + ":" + "M" + str(select_table_row) + ")"
        fix_summonth_cv_buy = "=SUM(N" + str(fix_sum_target_row) + ":" + "N" + str(select_table_row) + ")"
        selectworksheet.update_acell("L" + str(fix_sum_row), fix_summonth_cv_sell)
        selectworksheet.update_acell("M" + str(fix_sum_row), fix_summonth_cv_leaseback)
        selectworksheet.update_acell("N" + str(fix_sum_row), fix_summonth_cv_buy)
        selectworksheet.update_acell("O" + str(fix_sum_row), fix_summonth_doing)
    #FacebookシートのSUM設定
    elif selectworksheet == facebook_worksheet:
        expect_cv = "=I" + str(fix_sum_row) + "/" + str(end_date_date) + "*" + str(end_lastday)
        expect_cv_dif = "='目標'!R$" + str(end_date_month+1) + "-" + "P" + str(fix_sum_row)    
        fix_summonth_doing = "=K" + str(fix_sum_row) +"-'目標'!S$" + str(end_date_month+1)
        fix_summonth_cv_sell = "=SUM(L" + str(fix_sum_target_row) + ":" + "L" + str(select_table_row) + ")"
        fix_summonth_cv_leaseback = "=SUM(M" + str(fix_sum_target_row) + ":" + "M" + str(select_table_row) + ")"        
        fix_summonth_cv_buy = "=SUM(N" + str(fix_sum_target_row) + ":" + "N" + str(select_table_row) + ")"
        selectworksheet.update_acell("L" + str(fix_sum_row), fix_summonth_cv_sell)
        selectworksheet.update_acell("M" + str(fix_sum_row), fix_summonth_cv_leaseback)
        selectworksheet.update_acell("N" + str(fix_sum_row), fix_summonth_cv_buy)
        selectworksheet.update_acell("O" + str(fix_sum_row), fix_summonth_doing)
        selectworksheet.update_acell("P" + str(fix_sum_row), expect_cv)
        selectworksheet.update_acell("Q" + str(fix_sum_row), expect_cv_dif)
    #SmartnewsシートのSUM設定
    elif selectworksheet == smartnews_worksheet:
        expect_cv = "=I" + str(fix_sum_row) + "/" + str(end_date_date) + "*" + str(end_lastday)
        expect_cv_dif = "='目標'!T$" + str(end_date_month+1) + "-" + "P" + str(fix_sum_row)    
        fix_summonth_doing = "=K" + str(fix_sum_row) +"-'目標'!U$" + str(end_date_month+1)
        fix_summonth_cv_sell = "=SUM(L" + str(fix_sum_target_row) + ":" + "L" + str(select_table_row) + ")"
        fix_summonth_cv_leaseback = "=SUM(M" + str(fix_sum_target_row) + ":" + "M" + str(select_table_row) + ")"        
        fix_summonth_cv_buy = "=SUM(N" + str(fix_sum_target_row) + ":" + "N" + str(select_table_row) + ")"
        selectworksheet.update_acell("L" + str(fix_sum_row), fix_summonth_cv_sell)
        selectworksheet.update_acell("M" + str(fix_sum_row), fix_summonth_cv_leaseback)
        selectworksheet.update_acell("N" + str(fix_sum_row), fix_summonth_cv_buy)
        selectworksheet.update_acell("O" + str(fix_sum_row), fix_summonth_doing)
        selectworksheet.update_acell("P" + str(fix_sum_row), expect_cv)
        selectworksheet.update_acell("Q" + str(fix_sum_row), expect_cv_dif)
    #すべてのシート共通部分のSUMを記載
    selectworksheet.update_acell("D" + str(fix_sum_row), fix_summonth_budget)
    selectworksheet.update_acell("E" + str(fix_sum_row), fix_summonth_imp)
    selectworksheet.update_acell("F" + str(fix_sum_row), fix_summonth_click)
    selectworksheet.update_acell("G" + str(fix_sum_row), fix_summonth_ctr)
    selectworksheet.update_acell("H" + str(fix_sum_row), fix_summonth_cpc)
    selectworksheet.update_acell("I" + str(fix_sum_row), fix_summonth_cv)
    selectworksheet.update_acell("J" + str(fix_sum_row), fix_summonth_cvr)
    selectworksheet.update_acell("K" + str(fix_sum_row), fix_summonth_cpa)

def log_record():
    #----------ログの初期設定----------
    global logger
    logger = logging.getLogger('dtaillog')
    logger.setLevel(5)

    sh = logging.StreamHandler()
    logger.addHandler(sh)
    fh = logging.FileHandler("week_log/" + start_date.replace("/", "") + "_" + end_date.replace("/", "") + ".log")
    logger.addHandler(fh)
    return logger

 
#----------メイン関数(以下からのスクリプトが実行される)----------
if __name__ == "__main__":
    #----------合算用のカラムを整形----------
    colum = '媒体', '地域', '費用', '表示回数', 'クリック数', 'クリック率', 'クリック単価'
    #----------必要なカラムを整形----------
    colum_result = '費用', '表示回数', 'クリック数', 'クリック率', 'クリック単価', 'コンバージョン数', 'コンバージョン率', 'コンバージョン単価'
    colum_result_YG = '費用', '表示回数', 'クリック数', 'クリック率', 'クリック単価', 'コンバージョン数', 'コンバージョン率', 'コンバージョン単価', '社名CV'
    colum_result_other = '費用', '表示回数', 'クリック数', 'クリック率', 'クリック単価', 'コンバージョン数', 'コンバージョン率', 'コンバージョン単価', '売却CV', 'リースバックCV', '購入CV'
    
    #----------スプレッドシートの操作----------
    
    #----------スプレッドシートapiの情報を取得----------
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    
    #----------ローカルにあるjsonファイルを特定し、キーを取得----------
    credentials = ServiceAccountCredentials.from_json_keyfile_name('certain-mission-265610-63e3a1a7469e.json', scope)

    #----------操作するスプレッドシートを指定----------
    gc = gspread.authorize(credentials)
    SPREADSHEET_KEY = '1pcFenRxbjgOIGTzTcLVI3gM5wx1lDqxUJ-Mt7kNqRwA'
    workbook = gc.open_by_key(SPREADSHEET_KEY)

    #----------データを取得する期間をスプレッドシートから取得----------
    date_worksheet = workbook.worksheet('取得日')
    start_date = date_worksheet.acell("A4").value
    end_date = date_worksheet.acell("B4").value
    end_date_get_month = datetime.datetime.strptime(end_date, '%Y/%m/%d')
    end_date_month =  end_date_get_month.month
    end_date_date = end_date_get_month.day
    end_lastday = calendar.monthrange(2020, end_date_month)[1]
    
    #----------ログの出力----------
    log_record()
    logger.log ("取得開始日: " + start_date)
    logger.log ("終了日: " + end_date)
    
    #----------Excel整形関数実行----------
    excel_fix(webantena_path)
    excel_fix(G_path)
    excel_fix(G_area_path)
    excel_fix(YSS_area_path)
    excel_fix(YSS_path)
    excel_fix(YDN_path)
    excel_fix(YDN_area_path)
    excel_fix(criteo_path)
    excel_fix(facebook_path)
    excel_fix(smartnews_path)
    
    #----------整形した数値をルールを元に合算する----------
    #都心6区リスティングYG合算
    six_concat_df = pd.concat([GSS_df_six_brand_result, YSS_df_six_brand_result])
    six_sum_budget = six_concat_df['費用'].sum()
    six_sum_imp = six_concat_df['表示回数'].sum()
    six_sum_click = six_concat_df['クリック数'].sum()
    six_sum_ctr = six_sum_click/six_sum_imp
    six_sum_cpc = six_sum_budget/six_sum_click
    six_cvr = six_brand_cv/six_sum_click 
    six_cpa = six_sum_budget/six_brand_cv
    six_result = pd.DataFrame([[six_sum_budget, six_sum_imp, six_sum_click, six_sum_ctr, six_sum_cpc, six_brand_cv, six_cvr, six_cpa]], columns=(colum_result))
    six_result = six_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("都心6区リスティングYG合算")
    logger.log ("-----------------------")
    logger.log (GSS_df_six_brand_result)
    logger.log (YSS_df_six_brand_result)
    logger.log (six_result)
    logger.log ("-----------------------")
    
    #都心6区ディスプレイRMRTYG合算
    six_dn_concat_df = pd.concat([GDN_df_six_rm_result, YDN_df_six_rt_result])
    six_dn_sum_budget = six_dn_concat_df['費用'].sum()
    six_dn_sum_imp = six_dn_concat_df['表示回数'].sum()
    six_dn_sum_click = six_dn_concat_df['クリック数'].sum()
    six_dn_sum_ctr = six_dn_sum_click/six_dn_sum_imp
    six_dn_sum_cpc = six_dn_sum_budget/six_dn_sum_click
    six_dn_cvr = six_dn_cv/six_dn_sum_click 
    six_dn_cpa = six_dn_sum_budget/six_dn_cv
    six_dn_result = pd.DataFrame([[six_dn_sum_budget, six_dn_sum_imp, six_dn_sum_click, six_dn_sum_ctr, six_dn_sum_cpc, six_dn_cv, six_dn_cvr, six_dn_cpa]], columns=(colum_result))
    six_dn_result = six_dn_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("都心6区ディスプレイRMRTYG合算")
    logger.log ("-----------------------")
    logger.log (GDN_df_six_rm_result)
    logger.log (YDN_df_six_rt_result)
    logger.log (six_dn_result)
    logger.log ("-----------------------")

    #都心6区ディスプレイTG(Yahoo)
    YDN_df_six_tg_sum_ctr = YDN_df_six_tg_sum_click/YDN_df_six_tg_sum_imp
    YDN_df_six_tg_sum_cpc = YDN_df_six_tg_sum_budget/YDN_df_six_tg_sum_click
    YDN_df_six_tg_cvr = YDN_six_tg_cv/YDN_df_six_tg_sum_click
    YDN_df_six_tg_cpa = YDN_df_six_tg_sum_budget/YDN_six_tg_cv
    YDN_df_six_tg_result = pd.DataFrame([[YDN_df_six_tg_sum_budget, YDN_df_six_tg_sum_imp, YDN_df_six_tg_sum_click, YDN_df_six_tg_sum_ctr, YDN_df_six_tg_sum_cpc, YDN_six_tg_cv, YDN_df_six_tg_cvr, YDN_df_six_tg_cpa]], columns=(colum_result))
    YDN_df_six_tg_result = YDN_df_six_tg_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("都心6区YDNターゲット")
    logger.log ("-----------------------")
    logger.log (YDN_df_six_tg_result)
    logger.log ("-----------------------")
    
    #都心6区合計
    six_budget = six_sum_budget + six_dn_sum_budget + YDN_df_six_tg_sum_budget
    six_imp = six_sum_imp + six_dn_sum_imp + YDN_df_six_tg_sum_imp
    six_click = six_sum_click + six_dn_sum_click + YDN_df_six_tg_sum_click
    six_ctr = six_click/six_imp 
    six_cpc = six_budget/six_click
    six_cv_all = six_brand_cv + six_dn_cv + YDN_six_tg_cv
    six_cvr_all = six_cv_all/six_click 
    six_cpa_all = six_budget/six_cv_all
    six_result_all = pd.DataFrame([[six_budget, six_imp, six_click, six_ctr, six_cpc, six_cv_all, six_cvr_all, six_cpa_all, six_brandonly_cv]], columns=(colum_result_YG))
    six_result_all = six_result_all.replace(np.inf,np.nan).fillna(0)
    logger.log ("都心6区合算")
    logger.log ("-----------------------")
    logger.log (six_result_all)
    logger.log ("-----------------------")
    
    #注力エリアリスティングYG合算
    focus_concat_df = pd.concat([GSS_df_focus_brand_result, YSS_df_focus_brand_result])
    focus_sum_budget = focus_concat_df['費用'].sum()
    focus_sum_imp = focus_concat_df['表示回数'].sum()
    focus_sum_click = focus_concat_df['クリック数'].sum()
    focus_sum_ctr = focus_sum_click/focus_sum_imp 
    focus_sum_cpc = focus_sum_budget/focus_sum_click
    focus_cvr = focus_brand_cv/focus_sum_click 
    focus_cpa = focus_sum_budget/focus_brand_cv
    focus_result = pd.DataFrame([[focus_sum_budget, focus_sum_imp, focus_sum_click, focus_sum_ctr, focus_sum_cpc, focus_brand_cv, focus_cvr, focus_cpa]], columns=(colum_result))
    focus_result = focus_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("注力エリアリスティングYG合算")
    logger.log ("-----------------------")
    logger.log (GSS_df_focus_brand_result)
    logger.log (YSS_df_focus_brand_result)
    logger.log (focus_result)
    logger.log ("-----------------------")

    #注力エリアディスプレイRMRTYG合算
    focus_dn_concat_df = pd.concat([GDN_df_focus_rm_result, YDN_df_focus_rt_result])
    focus_dn_sum_budget = focus_dn_concat_df['費用'].sum()
    focus_dn_sum_imp = focus_dn_concat_df['表示回数'].sum()
    focus_dn_sum_click = focus_dn_concat_df['クリック数'].sum()
    focus_dn_sum_ctr = focus_dn_sum_click/focus_dn_sum_imp 
    focus_dn_sum_cpc = focus_dn_sum_budget/focus_dn_sum_click
    focus_dn_cvr = focus_dn_cv/focus_dn_sum_click 
    focus_dn_cpa = focus_dn_sum_budget/focus_dn_cv
    focus_dn_result = pd.DataFrame([[focus_dn_sum_budget, focus_dn_sum_imp, focus_dn_sum_click, focus_dn_sum_ctr, focus_dn_sum_cpc, focus_dn_cv, focus_dn_cvr, focus_dn_cpa]], columns=(colum_result))
    focus_dn_result = focus_dn_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("注力エリアディスプレイRMRTYG合算")
    logger.log ("-----------------------")
    logger.log (GDN_df_focus_rm_result)
    logger.log (YDN_df_focus_rt_result)
    logger.log (focus_dn_result)
    logger.log ("-----------------------")

    #注力エリアディスプレイTG(Yahoo)
    YDN_df_focus_tg_sum_ctr = YDN_df_focus_tg_sum_click/YDN_df_focus_tg_sum_imp
    YDN_df_focus_tg_sum_cpc = YDN_df_focus_tg_sum_budget/YDN_df_focus_tg_sum_click
    YDN_df_focus_tg_cvr = YDN_focus_tg_cv/YDN_df_focus_tg_sum_click
    YDN_df_focus_tg_cpa = YDN_df_focus_tg_sum_budget/YDN_focus_tg_cv
    YDN_df_focus_tg_result = pd.DataFrame([[YDN_df_focus_tg_sum_budget, YDN_df_focus_tg_sum_imp, YDN_df_focus_tg_sum_click, YDN_df_focus_tg_sum_ctr, YDN_df_focus_tg_sum_cpc, YDN_focus_tg_cv, YDN_df_focus_tg_cvr, YDN_df_focus_tg_cpa]], columns=(colum_result))
    YDN_df_focus_tg_result = YDN_df_focus_tg_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("注力エリアYDNターゲット")
    logger.log ("-----------------------")
    logger.log (YDN_df_focus_tg_result)
    logger.log ("-----------------------")

    #注力エリア合算
    focus_budget = focus_sum_budget + focus_dn_sum_budget + YDN_df_focus_tg_sum_budget
    focus_imp = focus_sum_imp + focus_dn_sum_imp + YDN_df_focus_tg_sum_imp
    focus_click = focus_sum_click + focus_dn_sum_click + YDN_df_focus_tg_sum_click
    focus_ctr = focus_click/focus_imp 
    focus_cpc = focus_budget/focus_click
    focus_cv_all = focus_brand_cv + focus_dn_cv + YDN_focus_tg_cv
    focus_cvr_all = focus_cv_all/focus_click
    focus_cpa_all = focus_budget/focus_cv_all
    focus_result_all = pd.DataFrame([[focus_budget, focus_imp, focus_click, focus_ctr, focus_cpc, focus_cv_all, focus_cvr_all, focus_cpa_all, focus_brandonly_cv]], columns=(colum_result_YG))
    focus_result_all = focus_result_all.replace(np.inf,np.nan).fillna(0)
    logger.log ("注力エリア合算")
    logger.log ("-----------------------")
    logger.log (focus_result_all)
    logger.log ("-----------------------")
    
    #その他エリアリスティングYG合算
    otherarea_concat_df = pd.concat([GSS_df_otherarea_brand_result, YSS_df_otherarea_brand_result])
    otherarea_sum_budget = otherarea_concat_df['費用'].astype(int).sum()
    otherarea_sum_imp = otherarea_concat_df['表示回数'].sum()
    otherarea_sum_click = otherarea_concat_df['クリック数'].astype(int).sum()
    otherarea_sum_ctr = otherarea_sum_click/otherarea_sum_imp 
    otherarea_sum_cpc = otherarea_sum_budget/otherarea_sum_click
    otherarea_cvr = otherarea_brand_cv/otherarea_sum_click 
    otherarea_cpa = otherarea_sum_budget/otherarea_brand_cv
    otherarea_result = pd.DataFrame([[otherarea_sum_budget, otherarea_sum_imp, otherarea_sum_click, otherarea_sum_ctr, otherarea_sum_cpc, otherarea_brand_cv, otherarea_cvr, otherarea_cpa]], columns=(colum_result))
    otherarea_result = otherarea_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("その他エリアリスティングYG合算")
    logger.log ("-----------------------")
    logger.log (GSS_df_otherarea_brand_result)
    logger.log (YSS_df_otherarea_brand_result)
    logger.log (otherarea_result)
    logger.log ("-----------------------")

    #その他エリアディスプレイRMRTYG合算
    otherarea_dn_concat_df = pd.concat([GDN_df_otherarea_rm_result, YDN_df_otherarea_rt_result])
    otherarea_dn_sum_budget = otherarea_dn_concat_df['費用'].astype(int).sum()
    otherarea_dn_sum_imp = otherarea_dn_concat_df['表示回数'].astype(int).sum()
    otherarea_dn_sum_click = otherarea_dn_concat_df['クリック数'].astype(int).sum()
    otherarea_dn_sum_ctr = otherarea_dn_sum_click/otherarea_dn_sum_imp 
    otherarea_dn_sum_cpc = otherarea_dn_sum_budget/otherarea_dn_sum_click
    otherarea_dn_cvr = otherarea_dn_cv/otherarea_dn_sum_click 
    otherarea_dn_cpa = otherarea_dn_sum_budget/otherarea_dn_cv
    otherarea_dn_result = pd.DataFrame([[otherarea_dn_sum_budget, otherarea_dn_sum_imp, otherarea_dn_sum_click, otherarea_dn_sum_ctr, otherarea_dn_sum_cpc, otherarea_dn_cv, otherarea_dn_cvr, otherarea_dn_cpa]], columns=(colum_result))
    otherarea_dn_result = otherarea_dn_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("その他エリアディスプレイRMRTYG合算")
    logger.log ("-----------------------")
    logger.log (GDN_df_otherarea_rm_result)
    logger.log (YDN_df_otherarea_rt_result)
    logger.log (otherarea_dn_result)
    logger.log ("-----------------------")

    #その他エリアディスプレイTG(Yahoo)
    YDN_df_otherarea_tg_sum_ctr = YDN_df_otherarea_tg_sum_click/YDN_df_otherarea_tg_sum_imp
    YDN_df_otherarea_tg_sum_cpc = YDN_df_otherarea_tg_sum_budget/YDN_df_otherarea_tg_sum_click
    YDN_df_otherarea_tg_cvr = YDN_otherarea_tg_cv/YDN_df_otherarea_tg_sum_click
    YDN_df_otherarea_tg_cpa = YDN_df_otherarea_tg_sum_budget/YDN_otherarea_tg_cv
    YDN_df_otherarea_tg_result = pd.DataFrame([[YDN_df_otherarea_tg_sum_budget, YDN_df_otherarea_tg_sum_imp, YDN_df_otherarea_tg_sum_click, YDN_df_otherarea_tg_sum_ctr, YDN_df_otherarea_tg_sum_cpc, YDN_otherarea_tg_cv, YDN_df_otherarea_tg_cvr, YDN_df_otherarea_tg_cpa]], columns=(colum_result))
    YDN_df_otherarea_tg_result = YDN_df_otherarea_tg_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("その他エリアYDNターゲット")
    logger.log ("-----------------------")
    logger.log (YDN_df_otherarea_tg_result)
    logger.log ("-----------------------")

    #その他合算
    otherarea_budget = otherarea_sum_budget + otherarea_dn_sum_budget + YDN_df_otherarea_tg_sum_budget
    otherarea_imp = otherarea_sum_imp + otherarea_dn_sum_imp + YDN_df_otherarea_tg_sum_imp
    otherarea_click = otherarea_sum_click + otherarea_dn_sum_click + YDN_df_otherarea_tg_sum_click
    otherarea_ctr = otherarea_click/otherarea_imp 
    otherarea_cpc = otherarea_budget/otherarea_click
    otherarea_cv_all = otherarea_brand_cv + otherarea_dn_cv + YDN_otherarea_tg_cv
    otherarea_cvr_all = otherarea_cv_all/otherarea_click 
    otherarea_cpa_all = otherarea_budget/otherarea_cv_all
    otherarea_result_all = pd.DataFrame([[otherarea_budget, otherarea_imp, otherarea_click, otherarea_ctr, otherarea_cpc, otherarea_cv_all, otherarea_cvr_all, otherarea_cpa_all, otherarea_brandonly_cv]], columns=(colum_result_YG))
    otherarea_result_all = otherarea_result_all.replace(np.inf,np.nan).fillna(0)
    logger.log ("その他エリア合算")
    logger.log ("-----------------------")
    logger.log (otherarea_result_all)
    logger.log ("-----------------------")
    
    #リースバックリスティングYG合算
    leaseback_concat_df = pd.concat([GSS_df_leaseback_brand_result, YSS_df_leaseback_brand_result])
    leaseback_sum_budget = leaseback_concat_df['費用'].astype(int).sum()
    leaseback_sum_imp = leaseback_concat_df['表示回数'].astype(int).sum()
    leaseback_sum_click = leaseback_concat_df['クリック数'].astype(int).sum()
    leaseback_sum_ctr = leaseback_sum_click/leaseback_sum_imp 
    leaseback_sum_cpc = leaseback_sum_budget/leaseback_sum_click
    leaseback_cvr = leaseback_brand_cv/leaseback_sum_click 
    leaseback_cpa = leaseback_sum_budget/leaseback_brand_cv
    leaseback_result = pd.DataFrame([[leaseback_sum_budget, leaseback_sum_imp, leaseback_sum_click, leaseback_sum_ctr, leaseback_sum_cpc, leaseback_brand_cv, leaseback_cvr, leaseback_cpa]], columns=(colum_result))
    leaseback_result = leaseback_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("リースバックリスティングYG合算")
    logger.log ("-----------------------")
    logger.log (GSS_df_leaseback_brand_result)
    logger.log (YSS_df_leaseback_brand_result)
    logger.log (leaseback_result)
    logger.log ("-----------------------")
    
    #リースバックディスプレイRMRTYG合算
    leaseback_dn_concat_df = pd.concat([GDN_df_leaseback_rm, YDN_df_leaseback_rt_result])
    leaseback_dn_sum_budget = leaseback_dn_concat_df['費用'].astype(int).sum()
    leaseback_dn_sum_imp = leaseback_dn_concat_df['表示回数'].astype(int).sum()
    leaseback_dn_sum_click = leaseback_dn_concat_df['クリック数'].astype(int).sum()
    leaseback_dn_sum_ctr = leaseback_dn_sum_click/leaseback_dn_sum_imp 
    leaseback_dn_sum_cpc = leaseback_dn_sum_budget/leaseback_dn_sum_click
    leaseback_dn_cvr = leaseback_dn_cv/leaseback_dn_sum_click 
    leaseback_dn_cpa = leaseback_dn_sum_budget/leaseback_dn_cv
    leaseback_dn_result = pd.DataFrame([[leaseback_dn_sum_budget, leaseback_dn_sum_imp, leaseback_dn_sum_click, leaseback_dn_sum_ctr, leaseback_dn_sum_cpc, leaseback_dn_cv, leaseback_dn_cvr, leaseback_dn_cpa]], columns=(colum_result))
    leaseback_dn_result = leaseback_dn_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("リースバックディスプレイRMRTYG合算")
    logger.log ("-----------------------")
    logger.log (GDN_df_leaseback_rm)
    logger.log (YDN_df_leaseback_rt_result)
    logger.log (leaseback_dn_result)
    logger.log ("-----------------------")

    #リースバックディスプレイTG(Yahoo)
    YDN_df_leaseback_tg_sum_ctr = YDN_df_leaseback_tg_sum_click/YDN_df_leaseback_tg_sum_imp
    YDN_df_leaseback_tg_sum_cpc = YDN_df_leaseback_tg_sum_budget/YDN_df_leaseback_tg_sum_click
    YDN_df_leaseback_tg_cvr = YDN_leaseback_tg_cv/YDN_df_leaseback_tg_sum_click
    YDN_df_leaseback_tg_cpa = YDN_df_leaseback_tg_sum_budget/YDN_leaseback_tg_cv
    YDN_df_leaseback_tg_result = pd.DataFrame([[YDN_df_leaseback_tg_sum_budget, YDN_df_leaseback_tg_sum_imp, YDN_df_leaseback_tg_sum_click, YDN_df_leaseback_tg_sum_ctr, YDN_df_leaseback_tg_sum_cpc, YDN_leaseback_tg_cv, YDN_df_leaseback_tg_cvr, YDN_df_leaseback_tg_cpa]], columns=(colum_result))
    YDN_df_leaseback_tg_result = YDN_df_leaseback_tg_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("リースバックYDNターゲット")
    logger.log ("-----------------------")
    logger.log (YDN_df_leaseback_tg_result)
    logger.log ("-----------------------")

    #リースバック合算
    leaseback_budget = leaseback_sum_budget + leaseback_dn_sum_budget + YDN_df_leaseback_tg_sum_budget
    leaseback_imp = leaseback_sum_imp + leaseback_dn_sum_imp + YDN_df_leaseback_tg_sum_imp
    leaseback_click = leaseback_sum_click + leaseback_dn_sum_click + YDN_df_leaseback_tg_sum_click
    leaseback_ctr = leaseback_click/leaseback_imp 
    leaseback_cpc = leaseback_budget/leaseback_click
    leaseback_cv_all = leaseback_brand_cv + leaseback_dn_cv + YDN_leaseback_tg_cv
    leaseback_cvr_all = leaseback_cv_all/leaseback_click 
    leaseback_cpa_all = leaseback_budget/leaseback_cv_all
    leaseback_result_all = pd.DataFrame([[leaseback_budget, leaseback_imp, leaseback_click, leaseback_ctr, leaseback_cpc, leaseback_cv_all, leaseback_cvr_all, leaseback_cpa_all, leaseback_brandonly_cv]], columns=(colum_result_YG))
    leaseback_result_all = leaseback_result_all.replace(np.inf,np.nan).fillna(0)
    logger.log ("リースバック合算")
    logger.log ("-----------------------")
    logger.log (leaseback_result_all)
    logger.log ("-----------------------")
    
    #社名・ブランド名YG合算
    brand_concat_df = pd.concat([GSS_df_brand_result, YSS_df_brand_result])
    brand_sum_budget = brand_concat_df['費用'].sum()
    brand_sum_imp = brand_concat_df['表示回数'].sum()
    brand_sum_click = brand_concat_df['クリック数'].sum()
    brand_sum_ctr = brand_sum_click/brand_sum_imp 
    brand_sum_cpc = brand_sum_budget/brand_sum_click
    brand_cvr = brand_cv/brand_sum_click 
    brand_cpa = brand_sum_budget/brand_cv
    brand_result = pd.DataFrame([[brand_sum_budget, brand_sum_imp, brand_sum_click, brand_sum_ctr, brand_sum_cpc, brand_cv, brand_cvr, brand_cpa]], columns=(colum_result))
    brand_result = brand_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("社名・ブランド名YG合算")
    logger.log ("-----------------------")
    logger.log (GSS_df_brand_result)
    logger.log (YSS_df_brand_result)
    logger.log (brand_result)
    logger.log ("-----------------------")
    
    #クリテオ
    logger.log ("クリテオ")
    logger.log ("-----------------------")
    logger.log (criteo_df)
    logger.log ("-----------------------")

    #Facebook
    logger.log ("Facebook")
    logger.log ("-----------------------")
    logger.log (facebook_df_result)
    logger.log ("-----------------------")

    #Smartnews
    logger.log ("Smartnews")
    logger.log ("-----------------------")
    logger.log (smartnews_df_result)
    logger.log ("-----------------------")
    
    logger.log ("売却流入からの買いたいコンバージョン数")
    logger.log ("-----------------------")
    logger.log (buywant_cv)
    logger.log ("-----------------------")

    #購入YG/criteo/FB/SN合算
    buy_concat_df = pd.concat([GSS_df_buy_result, YSS_df_buy_result])
    buy_sum_budget = buy_concat_df['費用'].sum()
    buy_sum_budget = buy_sum_budget + criteo_budget
    buy_cv = buy_cv + criteo_buy_cv + facebook_buy_cv + smartnews_buy_cv
    buy_cpa = buy_sum_budget/buy_cv
    buy_result = pd.DataFrame([[buy_sum_budget, buy_cv, buy_cpa]], columns=('費用', 'コンバージョン数', 'コンバージョン単価'))
    buy_result = buy_result.replace(np.inf,np.nan).fillna(0)
    logger.log ("購入YG/criteo/FB/SN合算")
    logger.log ("-----------------------")
    logger.log (GSS_df_buy_result)
    logger.log (YSS_df_buy_result)
    logger.log (buy_result)
    logger.log ("-----------------------")
   
    #YG全体サマリ
    YG_budget = six_budget + focus_budget + otherarea_budget + leaseback_budget
    YG_imp = six_imp + focus_imp + otherarea_imp + leaseback_imp
    YG_click = six_click + focus_click + otherarea_click + leaseback_click
    YG_ctr = YG_click/YG_imp 
    YG_cpc = YG_budget/YG_click
    YG_cv = six_cv_all + focus_cv_all + otherarea_cv_all + leaseback_cv_all
    YG_cvr = YG_cv/YG_click 
    YG_cpa = YG_budget/YG_cv
    YG_result_all = pd.DataFrame([[YG_budget, YG_imp, YG_click, YG_ctr, YG_cpc, YG_cv, YG_cvr, YG_cpa, brand_cv]], columns=(colum_result_YG))
    logger.log ("YG全体サマリ合算")
    logger.log ("-----------------------")
    logger.log (YG_result_all)
    logger.log ("-----------------------")

    #全体サマリ
    other_cv = criteo_sell_cv + criteo_leaseback_cv + facebook_sell_cv + facebook_leaseback_cv + smartnews_sell_cv + smartnews_leaseback_cv
    budget = YG_budget + facebook_sum_budget + smartnews_sum_budget
    cv = YG_cv + other_cv
    cpa = budget/cv
    result_all = pd.DataFrame([[budget, cv, cpa]], columns=('費用', 'コンバージョン数', 'コンバージョン単価'))
    logger.log ("全体サマリ合算")
    logger.log ("-----------------------")
    logger.log (result_all)
    logger.log ("-----------------------")
    
    
    #----------それぞれのシートにデータを反映----------
    #全体サマリ
    all_worksheet = workbook.worksheet('全体サマリ')
    add_data_all_buy(all_worksheet, result_all)
    time.sleep(20)

    #購入サマリ
    buy_worksheet = workbook.worksheet('購入サマリ')
    add_data_all_buy(buy_worksheet, buy_result)
    time.sleep(20)
    
    #YG全体サマリ
    YG_all_worksheet = workbook.worksheet('YG全体サマリ')
    YG_all_cell = YG_all_worksheet.findall(str(end_date_month) + '月計')
    YG_all_cell_list = []
    for YG_all_target_cell in YG_all_cell:
        YG_all_cell_list.append(YG_all_target_cell.row)
    YG_all_table_row = YG_all_cell_list[0]
    six_all_table_row = YG_all_cell_list[1] + 1
    focus_all_table_row = YG_all_cell_list[2] + 2
    otherarea_all_table_row = YG_all_cell_list[3] + 3
    leaseback_all_table_row = YG_all_cell_list[4] + 4
    
    #YG全体数値
    add_data_fix(YG_all_worksheet, YG_all_table_row, YG_result_all, "【全体】")
    time.sleep(20)
    
    #都心6区全体数値
    add_data_fix(YG_all_worksheet, six_all_table_row, six_result_all, "【都心6区】")
    time.sleep(20)
    
    #注力エリア全体数値
    add_data_fix(YG_all_worksheet, focus_all_table_row, focus_result_all, "【注力エリア】")
    time.sleep(20)
    
    #その他エリア全体数値
    add_data_fix(YG_all_worksheet, otherarea_all_table_row, otherarea_result_all, "【その他エリア】")
    time.sleep(20)
    
    #リースバック全体数値
    add_data_fix(YG_all_worksheet, leaseback_all_table_row, leaseback_result_all, "【リースバック】")
    time.sleep(20)
    
    #YG都心6区数値
    six_worksheet = workbook.worksheet('YG/都心６区')
    six_firstrow_cell = six_worksheet.findall(str(end_date_month) + '月計')
    six_cell_list = []
    for six_target_cell in six_firstrow_cell:
        six_cell_list.append(six_target_cell.row)
    six_risting_table_row = six_cell_list[0]
    six_displayrm_table_row = six_cell_list[1] + 1
    six_displaytg_table_row = six_cell_list[2] + 2
    
    #YG都心6区リスティング
    add_data_fix(six_worksheet, six_risting_table_row, six_result, "【リスティング】")
    time.sleep(20)

    #YG都心6区ディスプレイリマケ
    add_data_fix(six_worksheet, six_displayrm_table_row, six_dn_result, "【ディスプレイ_リマケ】")
    time.sleep(20)

    #YG都心6区ディスプレイターゲット
    add_data_fix(six_worksheet, six_displaytg_table_row, YDN_df_six_tg_result, "【ディスプレイ_ターゲ】")
    time.sleep(20)

    #YG注力エリア数値
    focus_worksheet = workbook.worksheet('YG/注力エリア')
    focus_firstrow_cell = focus_worksheet.findall(str(end_date_month) + '月計')
    focus_cell_list = []
    for focus_target_cell in focus_firstrow_cell:
        focus_cell_list.append(focus_target_cell.row)
    focus_risting_table_row = focus_cell_list[0]
    focus_displayrm_table_row = focus_cell_list[1] + 1
    focus_displaytg_table_row = focus_cell_list[2] + 2

    #YG注力エリアリスティング
    add_data_fix(focus_worksheet, focus_risting_table_row, focus_result, "【リスティング】")
    time.sleep(20)
    
    #YG注力エリアディスプレイリマケ
    add_data_fix(focus_worksheet, focus_displayrm_table_row, focus_dn_result, "【ディスプレイ_リマケ】")
    time.sleep(20)
    
    #YG注力エリアディスプレイターゲット
    add_data_fix(focus_worksheet, focus_displaytg_table_row, YDN_df_focus_tg_result, "【ディスプレイ_ターゲ】")
    time.sleep(20)
    
    #YGその他エリア数値
    otherarea_worksheet = workbook.worksheet('YG/その他エリア')
    otherarea_firstrow_cell = otherarea_worksheet.findall(str(end_date_month) + '月計')
    otherarea_cell_list = []
    for otherarea_target_cell in otherarea_firstrow_cell:
        otherarea_cell_list.append(otherarea_target_cell.row)
    otherarea_risting_table_row = otherarea_cell_list[0]
    otherarea_displayrm_table_row = otherarea_cell_list[1] + 1
    otherarea_displaytg_table_row = otherarea_cell_list[2] + 2

    #YGその他エリアリスティング
    add_data_fix(otherarea_worksheet, otherarea_risting_table_row, otherarea_result, "【リスティング】")
    time.sleep(20)

    #YGその他エリアディスプレイリマケ
    add_data_fix(otherarea_worksheet, otherarea_displayrm_table_row, otherarea_dn_result, "【ディスプレイ_リマケ】")
    time.sleep(20)
    
    #YGその他エリアディスプレイターゲット
    add_data_fix(otherarea_worksheet, otherarea_displaytg_table_row, YDN_df_otherarea_tg_result, "【ディスプレイ_ターゲ】")
    time.sleep(20)

    #YGリースバック数値
    leaseback_worksheet = workbook.worksheet('YG/リースバック')
    leaseback_firstrow_cell = leaseback_worksheet.findall(str(end_date_month) + '月計')
    leaseback_cell_list = []
    for leaseback_target_cell in leaseback_firstrow_cell:
        leaseback_cell_list.append(leaseback_target_cell.row)
    leaseback_risting_table_row = leaseback_cell_list[0]
    leaseback_displayrm_table_row = leaseback_cell_list[1] + 1
    leaseback_displaytg_table_row = leaseback_cell_list[2] + 2

    #YGリースバックリスティング
    add_data_fix(leaseback_worksheet, leaseback_risting_table_row, leaseback_result, "【リスティング】")
    time.sleep(20)

    #YGリースバックディスプレイリマケ
    add_data_fix(leaseback_worksheet, leaseback_displayrm_table_row, leaseback_dn_result, "【ディスプレイ_リマケ】")
    time.sleep(20)

    #YGリースバックディスプレイターゲット
    add_data_fix(leaseback_worksheet, leaseback_displaytg_table_row, YDN_df_leaseback_tg_result, "【ディスプレイ_ターゲ】")
    time.sleep(20)
   
    #YG社名・ブランド名数値
    brand_worksheet = workbook.worksheet('YG/社名・ブランド名')
    brand_firstrow_cell = brand_worksheet.find(str(end_date_month) + '月計')
    brand_risting_table_row = brand_firstrow_cell.row

    #YG社名・ブランド名リスティング
    add_data_fix(brand_worksheet, brand_risting_table_row, brand_result, "【リスティング】")
    time.sleep(20)
 
    #Criteo数値
    criteo_worksheet = workbook.worksheet('Criteo')
    criteo_firstrow_cell = criteo_worksheet.find(str(end_date_month) + '月計')
    criteo_all_table_row = criteo_firstrow_cell.row

    #Criteo全体
    add_data_fix(criteo_worksheet, criteo_all_table_row, criteo_df, "【全体】")
    time.sleep(20)
    
    #Facebook数値
    facebook_worksheet = workbook.worksheet('Facebook')
    facebook_firstrow_cell = facebook_worksheet.find(str(end_date_month) + '月計')
    facebook_all_table_row = facebook_firstrow_cell.row

    #Facebook全体
    add_data_fix(facebook_worksheet, facebook_all_table_row, facebook_df_result, "【全体】")
    time.sleep(20)

    #Smartnews数値
    smartnews_worksheet = workbook.worksheet('Smartnews')
    smartnews_firstrow_cell = smartnews_worksheet.find(str(end_date_month) + '月計')
    smartnews_all_table_row = smartnews_firstrow_cell.row

    #Smartnews全体
    add_data_fix(smartnews_worksheet, smartnews_all_table_row, smartnews_df_result, "【全体】")