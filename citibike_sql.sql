/************************************************************************************************
* 事前準備
************************************************************************************************/

//ロールやウェアハウスの設定
//スライド：ロール
USE ROLE ACCOUNTADMIN;
-- USE WAREHOUSE COMPUTE_WH;

//WAREHOUSEを作成してみよう 
//スライド：ウェアハウス
//ナビゲーションメニュー -> [Warehouses]

//SQLでの作成バージョン
-- CREATE OR REPLACE WAREHOUSE CITIBIKE_WH
-- WITH
--   WAREHOUSE_TYPE = STANDARD --WHのタイプ
--   WAREHOUSE_SIZE = XSMALL --WHのサイズ
--   MAX_CLUSTER_COUNT = 10 --最大クラスタ数
--   MIN_CLUSTER_COUNT = 1 --最小クラスタ数
--   SCALING_POLICY = STANDARD --クラスタスケールアウト/イン時の挙動
--   AUTO_SUSPEND = 1 --クエリ負荷がなくなってからサスペンドするまでの時間
--   AUTO_RESUME = TRUE; --クエリ負荷の入力で起動するか否か

SHOW WAREHOUSES;

USE WAREHOUSE CITIBIKE_WH;

//データベースCitibikeを作成しよう
CREATE OR REPLACE DATABASE CITIBIKE;

//コンテキストの設定
USE DATABASE CITIBIKE;
USE SCHEMA PUBLIC;

/************************************************************************************************
* データロードの準備
************************************************************************************************/

//スライド：データロード

//ロード対象のテーブル：tripsを作成しよう
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

//外部ステージを作成しよう
create or replace stage citibike_trips
 url = 's3://snowflake-workshop-lab/japan/citibike-trips/'
 -- credentials = ('aws_secret_key = '<key>' aws_key_id = '<id>')
;


//メモ：外部ステージの作成方法その２
//Storage Inregration（ストレージ統合）を利用した作成
//ストレージ統合：予めSnowflakeへ統合するストレージのロケーション情報,ストレージへの権限や認証情報を予めオブジェクト化できる

-- CREATE STORAGE INTEGRATION kfukamori_load_integration
-- TYPE = EXTERNAL_STAGE
-- STORAGE_PROVIDER = 'S3'
-- ENABLED = TRUE
-- STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::484577546576:role/kfukamori_load_role'
-- STORAGE_ALLOWED_LOCATIONS = ('s3://kfukamori-load/');

//ストレージ統合を利用した外部ステージの作成
-- CREATE OR REPLACE STAGE load_stage
-- STORAGE_INTEGRATION = kfukamori_load_integration
-- URL = 's3://kfukamori-load/'
-- FILE_FORMAT = kfukamori_csv_format
-- ;

//参考リンク
//https://docs.snowflake.com/user-guide/data-load-s3


//ステージ上にあるファイルをリスト
list @citibike_trips;


//ファイルフォーマットの作成
create or replace file format csv type='csv'
  compression = 'auto' 
  field_delimiter = ',' --各項目（列）がカンマ (,) で区切られていることを指定
  record_delimiter = '\n' --各レコード（行）が改行コード (\n) で区切られていることを指定
  skip_header = 0 --ファイルの先頭行をスキップしないことを指定します。もし 1 を指定すると、CSVファイルの1行目（ヘッダー行）を無視してデータを取り込み
  field_optionally_enclosed_by = '\042' --項目がダブルクォーテーションで囲まれている場合があることを指定します。\042はダブルクォーテーションの8進数表現。
  trim_space = false --項目の前後にある余分なスペースを削除しないよう
  error_on_column_count_mismatch = false --入力ファイルの区切り列の数が対応するテーブルの列の数と一致しない場合に、解析エラーを生成するかどうかを指定するブール値。
  escape = 'none' --ダブルクォーテーションで囲まれた項目内でのエスケープ文字を指定しない設定
  escape_unenclosed_field = '\134' --ダブルクォーテーションで囲まれていない項目内で、バックスラッシュ (\) をエスケープ文字として扱います。\134はバックスラッシュの8進数表現です
  date_format = 'auto' --日付のフォーマットを自動で判別
  timestamp_format = 'auto' --タイムスタンプ（日時）のフォーマットを自動で判別。
  null_if = ('') --項目が空の文字列 ('') だった場合、その値を**NULL** として扱います。
  comment = 'file format for ingesting data for zero to snowflake';


//作成されたファイルフォーマットの確認
show file formats in database citibike;


//ウェアハウスのサイズを変更（XSMALL -> SMALL）
alter warehouse CITIBIKE_WH set warehouse_size = SMALL;

show warehouses;


/************************************************************************************************
* データロード
************************************************************************************************/

//Snowflakeへのデータロード
//Copy intoコマンドの利用（ステージ上のファイルをTripsテーブルへロード）
COPY INTO trips FROM @citibike_trips 
FILE_FORMAT = CSV
PATTERN = '.*csv.*';

///ロードに要した時間のメモ
// 39 sec

//テーブルの確認
select * from trips limit 10;

//一度テーブルをトランケート（中身を削除）する
truncate table trips;

//中身が空になったことを確認
select * from trips limit 10;
  
//ウェアハウスサイズの変更（Largeサイズへ）
//Small -> Medium （リソースが2倍となる）
alter warehouse CITIBIKE_WH set warehouse_size='Medium';

//ウェアハウスサイズが変更されたことの確認
show warehouses;

//再度データをCitibikeテーブルへロード
copy into trips from @citibike_trips
file_format=CSV;

//要した時間のメモ
// 17 sec

//テーブルにデータが入ったことの確認
select * from trips limit 20;

//テーブルのレコード数をカウント
select count(*) from trips;


//例：Snowpipeを使ったデータロード
//Snowpipe：オブジェクトストレージにファイルがPutされたことをTriggerとして自動ロード
-- CREATE PIPE kfukamori_pipe
-- AS
-- COPY INTO citibike.public.trips
-- FROM @mystage
-- FILE_FORMAT = (TYPE = 'JSON');

//Snowpipe参考リンク
//https://docs.snowflake.com/ja/user-guide/data-load-snowpipe-auto-s3

/************************************************************************************************
* 分析クエリの発行
************************************************************************************************/

//分析クエリを発行して、統計情報を確認

//各時間の走行回数、平均走行時間、平均走行距離が表示
//実行時間をちぇっく！　実行時間：
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

//SQLをダッシュボードへ貼り付けてみよう
//ナビゲーションメニュー ->[Projects] -> [Dashboard]

//同じQueryをもう一度発行
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

//クエリリザルトキャッシュが利いている！！


//分析クエリ２
//月毎のTrips数の集計
select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

/************************************************************************************************
* ゼロコピークローン
************************************************************************************************/

//スライド：マイクロパーティション
//スライド：ゼロコピークローン

//ゼロコピークローンの利用
//どの程度時間がかかる？？
create or replace table trips_dev clone trips;

//trips_devテーブルが作成されたことをオブジェクトツリーから確認

//クローンしたテーブルをアップデートしよう
update citibike.public.trips_dev set tripduration = 0;

//元データとクローンデータの比較
select * from citibike.public.trips_dev limit 10;
select * from citibike.public.trips limit 10;


/************************************************************************************************
* 分析クエリの発行（その２）：半構造化データの処理
************************************************************************************************/


//データベースweatherの作成
create or replace database weather;


//コンテキストの設定
-- use role sysadmin;
-- use warehouse compute_wh;
use database weather;
use schema public;

//json_weather_dataテーブルの作成
create or replace table json_weather_data (v variant);

//外部ステージの作成
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

//外部ステージ上のファイルを確認
list @nyc_weather;

//データをjson_weather_dataテーブルへロード
copy into json_weather_data
from @nyc_weather 
file_format = (type = json strip_outer_array = true); --strip_outer_array:一番外側の[]を取り外す

//データを確認
select * from json_weather_data limit 10;

//スライド：半構造化データの展開

// ビューの作成
// ビュー作成時にJson半構造を各カラムへ構造化して展開する
create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';

//ネストされたJsonファイルの展開
//参考リンク
//https://zenn.dev/kommy339/articles/a5f768f820fbdb


//作成されたビューの確認
select * from json_weather_data_view
order by observation_time desc;

//特定の日のみを表示
//Observation_timeを月でTruncして2018年1月のみのデータを表示
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2019-03-01'
limit 20;

//もう一度Tripsテーブルを見てみよう
//starttimeカラムの確認、および
select * from citibike.public.trips
limit 10;

//Tripsテーブルとjson_weather_dataビューをJoinしよう
//Tripsテーブルのstarttimeと、json_weather_dataビューを時間でJoinする
select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join weather.public.json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;

/************************************************************************************************
* タイムトラベル
************************************************************************************************/

//スライド：タイムトラベル

///タイムトラベルの利用
//json_weather_dataテーブルをドロップしよう
drop table json_weather_data;

//json_weather_dataテーブルをSELECT
select * from json_weather_data limit 10;

//タイムトラベルその１
//undropコマンドで最新状態をリストア
undrop table json_weather_data;

//json_weather_dataテーブルをSELECT
select * from json_weather_data limit 10;

//json_weather_dataテーブルを書き換え
//クエリIDを記録しておこう
//Query ID: 
update trips set start_station_name = 'oops';

//書き換わったことを確認
select * from trips limit 10;

//書き換わったことを確認（その２）
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

//最新のUpdateクエリのクエリIDを取得
-- set query_id =
-- (select query_id from table(information_schema.query_history_by_session (result_limit=>5))
-- where query_text like 'update%' order by start_time desc limit 1);

//タイムトラベルその２
//特定のクエリIDの実行前の状態をリストア
create or replace table trips as
(select * from trips before (statement => '<Query_ID>'));

//リストアされていることを確認
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


/************************************************************************************************
* パイプラインの構築：Dynamic Tableの利用
************************************************************************************************/


-- * このセクションでは、走行データ(trips)と気象データ(weather)を結合し、
-- * 天候別の走行回数を自動集計するDynamic Tableを作成します。
-- * これにより、宣言的な記述だけでデータマートが自動的に最新の状態に保たれることを確認。


-- コンテキストの設定
-- Dynamic Tableを作成するデータベースとスキーマに切り替えます。
USE WAREHOUSE CITIBIKE_WH;
USE DATABASE CITIBIKE;
USE SCHEMA PUBLIC;


-- Dynamic Tableの作成
-- tripsテーブルとweatherデータベースのビューをJOINし、天候ごとの走行回数を集計します。
CREATE OR REPLACE DYNAMIC TABLE trips_by_weather_summary
    TARGET_LAG = '1 minutes' --TARGET_LAG: データの鮮度を定義します。ソースデータが変更されてから、1分以内にこのテーブルに反映される。
    WAREHOUSE = CITIBIKE_WH
AS
//先ほどのQueryをAS以下に指定
select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join weather.public.json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


-- データの確認
-- Cloudyの時のTrip数：
SELECT *
FROM trips_by_weather_summary
where conditions = 'Cloudy';


-- ソーステーブルへのデータ追加（新しい走行データのトランザクションデータが生成されたと仮定）
-- ここで、元の生データテーブル(`trips`)に、特定の時間帯の新しい走行データを手動でINSERT。
INSERT INTO citibike.public.trips (tripduration, starttime, stoptime, start_station_id, end_station_id, bikeid, usertype, birth_year, gender)
VALUES
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2),
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2),
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2),
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2),
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2),
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2),
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2),
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2),
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2),
(269, '2019-03-01 00:00:00.000', '2019-03-01 00:30:00.000', 471, 3077, 20828, 'Subscriber', 1983, 2)
;

-- Dynamic Tableが更新されるか確認しよう！！
SELECT *
FROM trips_by_weather_summary
where conditions = 'Cloudy';


/************************************************************************************************
* CORTEX AISQLの利用
************************************************************************************************/

-- Snowfsightからレビューデータをロードしよう！
-- 自動スキーマ検出を体験

///ロードしたデータの確認
SELECT * FROM citibike.public.user_reviews;

///Cortex AISQLを利用してみよう！！

--スライド：Cortex AIASQL

//Complete関数
SELECT
  *,
  SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-7-sonnet', -- 使用するAIモデル
    CONCAT('レビューの中でのキーワードをいくつか抜き出してください。なお、キーワードの単語そのもの以外の余計な返答は不要です。', 
    '対象レビューは次のとおり。',REVIEW_TEXT) -- AIへの指示（プロンプト）。及び目的のカラム
  ) AS extracted_word
FROM
  citibike.public.user_reviews; 


SELECT
  *,
  SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-7-sonnet', -- 使用するAIモデル
    CONCAT('レビューに対する自動応答案を生成してください。応答案本文以外の余計な返答は不要です。',
    'レビューには以下の3点を含めてください',
    '1. 冒頭の挨拶',
    '2. 感謝または謝罪',
    '3. 具体的な改善案',
    '対象のレビューは次のとおり。', REVIEW_TEXT) -- AIへの指示（プロンプト）。及び目的のカラム
  ) AS extracted_word
FROM
  citibike.public.user_reviews; 

//CLASSIFY関数
select
  *,
  AI_CLASSIFY(REVIEW_TEXT, ['サポートセンター関連','自転車の整備状態関連', 'アプリ関連', '金額関連','その他']):labels[0]::string as CATEGORY
from  citibike.public.user_reviews;


//SENTIMENT関数
SELECT
  *, 
  SNOWFLAKE.CORTEX.SENTIMENT(SNOWFLAKE.CORTEX.TRANSLATE(review_text, 'ja', 'en')) AS sentiment_score -- 感情分析スコア
FROM
   citibike.public.user_reviews; 


//ENTITY_SENTIMENT関数
SELECT 
*,
SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(SNOWFLAKE.CORTEX.TRANSLATE(review_text, 'ja', 'en'), ['Support Service','Bicycle', 'Application', 'Pricing']),
FROM citibike.public.user_reviews
LIMIT 10;

//AI_FILTER関数
select
  *
from
  citibike.public.user_reviews
where
  ai_filter(prompt('The review from a customer is related to pricing：{0}', SNOWFLAKE.CORTEX.TRANSLATE(review_text, 'ja', 'en'))) = True
;

/************************************************************************************************
* データ共有
************************************************************************************************/

//スライドデータ共有

//左側ナビゲーションメニュー -> [Private Sharing]
//必要な項目を入力 -> [Publish]

//Shareされたデータを確認しよう！
//[Private Sharing] -> [Shared with you] -> 共有を選択 -> [Get]

//共有されたデータにクエリをしよう
select * from <SHARED_DB_NAME>.public.json_weather_data_view
limit 10;

//共有しているデータをUpdateしよう
//元データ確認
select * from citibike.public.trips limit 10;

//アップデート実行
update trips set start_station_name = 'oops';

//共有されたデータへクエリしよう
//データの変更がリアルタイムに変更されている
select * from <Shared_DB_NAME>.public.trips limit 10;

//共有データをリストア
//タイムオフセットでのリストア
CREATE OR REPLACE TABLE citibike.public.trips AS
SELECT * FROM citibike.public.trips AT(OFFSET => -60*10);

///例:Direct Share方式
// シェアオブジェクトの作成
-- create or replace share citibike_ds_from_fukamori COMMENT = 'ダイレクト共有方式でのShare';
-- show shares in account;
-- -- use role securityadmin;
-- // シェアオブジェクトに対して、共有する対象のオブジェクトをGrantする
-- grant usage on database citibike TO SHARE citibike_ds_from_fukamori;
-- grant usage on schema citibike.public TO SHARE citibike_ds_from_fukamori;
-- grant select on all tables in schema citibike.public TO SHARE citibike_ds_from_fukamori;
-- grant select on all views in schema citibike.public TO SHARE citibike_ds_from_fukamori;

-- -- use role sysadmin;
-- //シェアオブジェクトを対象アカウントへADDする
-- alter share citibike_ds_from_fukamori ADD ACCOUNTS= <ACCOUNT_LOCATOR>;

-- //シェアのDrop
-- show shares;
-- drop share citibike_from_fukamori_1;
-- drop share citibike_from_fukamori_2;
-- drop share citibike_fukamori;
