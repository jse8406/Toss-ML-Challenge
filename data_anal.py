# age_group와 gender가 동시에 None인 row 한 개 예시 출력
def pl_print_both_null_row_example(parquet_path):
	lf = pl.scan_parquet(parquet_path)
	# 조건에 맞는 row 중 첫 번째만 수집
	row = lf.filter(pl.col('age_group').is_null() & pl.col('gender').is_null()).limit(1).collect()
	print('age_group와 gender가 동시에 None인 row 예시:')
	print(row)
# age_group와 gender가 동시에 None인 row 개수 출력
def pl_print_both_null_count(parquet_path):
	lf = pl.scan_parquet(parquet_path)
	both_null = lf.filter(pl.col('age_group').is_null() & pl.col('gender').is_null()).select(pl.count()).collect().item()
	print(f"age_group와 gender가 동시에 None인 row 개수: {both_null}")
# polars: gender 컬럼 카디널리티
def pl_get_gender_cardinality(parquet_path):
	lf = pl.scan_parquet(parquet_path)
	cardinality = lf.select(pl.col('gender')).unique().count().collect().item()
	print(f"Polars gender 컬럼의 카디널리티(고유값 개수): {cardinality}")
	unique_values = lf.select(pl.col('gender')).unique().collect()['gender'].to_list()
	# None이 있으면 정렬 시 에러가 나므로, str로 변환 후 정렬
	unique_values_sorted = sorted(unique_values, key=lambda x: (str(x) if x is not None else ''))
	print("gender 컬럼의 고유값(정렬):", unique_values_sorted)
	# None(null) 값 개수 출력
	null_count = lf.select(pl.col('gender').is_null().sum()).collect().item()
	print(f"gender 컬럼의 None(null) 개수: {null_count}")
def pl_get_age_cardinality(parquet_path):
	lf = pl.scan_parquet(parquet_path)
	cardinality = lf.select(pl.col('age_group')).unique().count().collect().item()
	print(f"Polars age 컬럼의 카디널리티(고유값 개수): {cardinality}")
	unique_values = lf.select(pl.col('age_group')).unique().collect()['age_group'].to_list()
	unique_values_sorted = sorted(unique_values, key=lambda x: (str(x) if x is not None else ''))
	print("age 컬럼의 고유값(정렬):", unique_values_sorted)
	# None(null) 값 개수 출력
	null_count = lf.select(pl.col('age_group').is_null().sum()).collect().item()
	print(f"age 컬럼의 None(null) 개수: {null_count}")
# pyarrow: gender 컬럼 카디널리티
def get_gender_cardinality(parquet_path):
	table = pq.read_table(parquet_path, columns=['gender'])
	df = table.to_pandas()
	cardinality = df['gender'].nunique()
	print(f"gender 컬럼의 카디널리티(고유값 개수): {cardinality}")

# polars 기반 함수 예시
import polars as pl

# polars: 컬럼명 출력
def pl_print_column_names(parquet_path):
	lf = pl.scan_parquet(parquet_path)
	print('Polars 컬럼명 목록:')
	print(lf.columns)

# polars: 컬럼 타입 출력
def pl_print_column_types(parquet_path):
	lf = pl.scan_parquet(parquet_path)
	print('Polars 컬럼별 데이터 타입:')
	print(lf.schema)

# polars: seq 컬럼 카디널리티
def pl_get_seq_cardinality(parquet_path):
	lf = pl.scan_parquet(parquet_path)
	# lazy 연산, collect()로 실행
	cardinality = lf.select(pl.col('seq')).unique().count().collect().item()
	print(f"Polars seq 컬럼의 카디널리티(고유값 개수): {cardinality}")

# polars: seq 컬럼 head 10개
def pl_print_seq_head(parquet_path):
	lf = pl.scan_parquet(parquet_path)
	head = lf.select('seq').limit(10).collect()
	print('Polars seq 컬럼 head 10개:')
	print(head['seq'].to_list())
# 각 컬럼의 데이터 타입 출력 함수
def print_column_types(parquet_path):
	parquet_file = pq.ParquetFile(parquet_path)
	print('컬럼별 데이터 타입:')
	for name, field in zip(parquet_file.schema.names, parquet_file.schema):
		print(f"{name}: {field.physical_type}")


# 필요한 라이브러리 import
import pyarrow.parquet as pq
import pandas as pd

# 파일 경로 정의
file_path = r'C:\Users\jse\Downloads\dataset\train.parquet'

# 데이터의 column 명만 출력하는 함수
def print_column_names(parquet_path):
	parquet_file = pq.ParquetFile(parquet_path)
	print('컬럼명 목록:')
	print(parquet_file.schema.names)

# seq 컬럼의 카디널리티(고유값 개수) 구하는 함수
def get_seq_cardinality(parquet_path):
	table = pq.read_table(parquet_path, columns=['seq'])
	df = table.to_pandas()
	cardinality = df['seq'].nunique()
	print(f"seq 컬럼의 카디널리티(고유값 개수): {cardinality}")

# seq 컬럼의 head 10개 출력 함수
def print_seq_head(parquet_path):
	parquet_file = pq.ParquetFile(parquet_path)
	# 첫 번째 row group에서 10개만 읽기 (iter_batches 사용)
	batch = next(parquet_file.iter_batches(columns=['seq'], batch_size=10))
	df = batch.to_pandas()
	print('seq 컬럼 head 10개:')
	print(df['seq'].to_list())

# 실행 예시
if __name__ == "__main__":
	# pyarrow 기반 예시
	# print_column_names(file_path)
	# get_seq_cardinality(file_path)
	# print_column_types(file_path)
	# print_seq_head(file_path)

	# # polars 기반 예시
	# pl_print_column_names(file_path)
	# pl_print_column_types(file_path)
	# pl_get_seq_cardinality(file_path)
	# pl_print_seq_head(file_path)
	pl_get_age_cardinality(file_path)
	pl_get_gender_cardinality(file_path)
	# pl_print_both_null_count(file_path)
	# pl_print_both_null_row_example(file_path)
	# get_gender_cardinality(file_path)