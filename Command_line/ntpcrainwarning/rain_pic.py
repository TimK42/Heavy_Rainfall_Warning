import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd


def df_plt(temp_df, current_time):
    # plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']  # 設置中文字體
    # mpl.rcParams['axes.unicode_minus'] = False
    plt.rcParams['font.sans-serif'] = ['Taipei Sans TC Beta']
    # DataFrame=>png
    # plt.figure('Qpesums雨量表')            # 視窗名稱
    ax = plt.axes(frame_on=False)  # 不要額外框線
    ax.xaxis.set_visible(False)  # 隱藏X軸刻度線
    ax.yaxis.set_visible(False)  # 隱藏Y軸刻度線
    plt.title('\n' + current_time + '前20名雨量站\n(依10分鐘雨量及1小時雨量排序)')
    pd.plotting.table(ax, temp_df[['鄉鎮', '雨量站', '測站高度', '10分鐘', '1小時', '3小時']], loc='center')  # 將df投射到ax上，且放置於ax的中間
    # plt.savefig('rain_pic/'+datetime.now().strftime('%Y%m%d%H%M%S')+'rain.png',dpi=200)     # 存檔
    plt.savefig('./rain.png', dpi=200)  # 存檔
    # plt.show()
    plt.close()
    return 'rain.png'
