import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pyarrow
import gc # 가비지 컬렉터(Garbage Collector) 모듈 import
import pyarrow.parquet as pq

# --------------------------------------------------------------------------
# 그래프에 한글 폰트를 설정합니다.
# --------------------------------------------------------------------------
try:
    plt.rcParams['font.family'] = 'Malgun Gothic' # Windows
    # plt.rcParams['font.family'] = 'NanumGothic' # Colab에 나눔고딕이 설치된 경우
    plt.rcParams['axes.unicode_minus'] = False
except:
    print("한글 폰트를 설정할 수 없습니다. 그래프의 한글이 깨질 수 있습니다.")
# --------------------------------------------------------------------------


# --- 1. 사전 준비: 분석할 컬럼 목록 정의 ---
# 파일 경로를 지정합니다.
file_path = r'C:\Users\jse\Downloads\dataset/train.parquet'

# 분석에 사용할 숫자형/범주형 컬럼 이름을 미리 정의합니다.
numerical_cols = [col for col in pq.read_schema(file_path).names if col.startswith('l_feat')]
categorical_cols = ['gender', 'age_group', 'inventory_id', 'day_of_week', 'hour']


# --- 2. 숫자형(Numerical) 변수 분석 (메모리 최적화) ---
print("--- [분석 1] 숫자형 변수 분석 시작 ---")
df_numerical = None # 변수 초기화
try:
    # ✨ 핵심: 숫자형 컬럼과 'clicked' 컬럼만 메모리로 불러옵니다.
    numerical_cols_to_load = numerical_cols + ['clicked']
    print(f"{len(numerical_cols_to_load)}개의 숫자형 관련 컬럼을 불러오는 중...")
    df_numerical = pd.read_parquet(file_path, columns=numerical_cols_to_load)
    print("불러오기 완료!")

    # 'clicked'와 숫자형 변수들 간의 상관계수를 계산합니다.
    correlations = df_numerical[numerical_cols].corrwith(df_numerical['clicked']).sort_values(ascending=False)
    print("\n상관계수 (상위 5개):")
    print(correlations.head(5))
    print("\n상관계수 (하위 5개):")
    print(correlations.tail(5))
    print("-" * 50)

    # 상관계수 절댓값이 가장 높은 상위 2개 변수를 찾아 박스플롯으로 시각화합니다.
    top_corr_features = correlations.abs().nlargest(2).index

    plt.figure(figsize=(14, 6))
    plt.suptitle("숫자형 주요 변수와 클릭 여부 관계", fontsize=16)
    for i, col in enumerate(top_corr_features):
        plt.subplot(1, 2, i+1)
        sns.boxplot(x='clicked', y=col, data=df_numerical, palette='viridis')
        plt.title(f"'clicked' 여부에 따른 '{col}' 분포", fontsize=12)
        plt.xlabel("클릭 여부 (0: No, 1: Yes)")
        plt.ylabel(col)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('numerical_feature_analysis.png')
    print("분석 결과가 'numerical_feature_analysis.png' 이미지 파일로 저장되었습니다.\n")

finally:
    # ✨ 핵심: 분석이 끝난 데이터프레임을 메모리에서 완전히 삭제합니다.
    del df_numerical
    gc.collect()
    print("숫자형 데이터 메모리 해제 완료!\n")


# --- 3. 범주형(Categorical) 변수 분석 (메모리 최적화) ---
print("--- [분석 2] 범주형 변수 분석 시작 ---")
df_categorical = None # 변수 초기화
try:
    # ✨ 핵심: 범주형 컬럼과 'clicked' 컬럼만 새로 불러옵니다.
    categorical_cols_to_load = categorical_cols + ['clicked']
    print(f"{len(categorical_cols_to_load)}개의 범주형 관련 컬럼을 불러오는 중...")
    df_categorical = pd.read_parquet(file_path, columns=categorical_cols_to_load)
    print("불러오기 완료!")

    # 분석할 주요 범주형 변수를 선택합니다.
    main_categorical_cols = ['gender', 'age_group', 'day_of_week']

    plt.figure(figsize=(18, 6))
    plt.suptitle("범주형 변수별 평균 클릭률(CTR)", fontsize=16)
    for i, col in enumerate(main_categorical_cols):
        plt.subplot(1, 3, i+1)
        ctr = df_categorical.groupby(col)['clicked'].mean().sort_values(ascending=False)
        sns.barplot(x=ctr.index, y=ctr.values, palette='plasma', order=ctr.index)
        plt.title(f"'{col}' 별 평균 클릭률", fontsize=14)
        plt.ylabel("평균 클릭률 (CTR)", fontsize=12)
        plt.xlabel(col, fontsize=12)
        plt.xticks(rotation=45)
        if not ctr.empty:
            plt.ylim(0, max(ctr.values) * 1.2)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('categorical_feature_analysis.png')
    print("분석 결과가 'categorical_feature_analysis.png' 이미지 파일로 저장되었습니다.")
    
finally:
    # ✨ 핵심: 분석이 끝난 데이터프레임을 메모리에서 완전히 삭제합니다.
    del df_categorical
    gc.collect()
    print("범주형 데이터 메모리 해제 완료!")

print("\n" + "=" * 50)
print("모든 분석이 정상적으로 완료되었습니다.")
print("=" * 50)