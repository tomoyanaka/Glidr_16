import pandas as pd
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ユーザーから入力を受け取る
start_column = int(input("読み取りを開始する列番号（1から開始）: ")) - 2  # Pandasは0から列を数えるので、1を引く
first_row = int(input("最初の行番号（1から開始）: ")) - 2  # 同様に、1を引く
last_row = int(input("最後の行番号（1から開始）: ")) - 2  # 同様に、1を引く
category_row = int(input("項目の行番号（1から開始）: ")) - 2  # 同様に、1を引く
target_category = input("チェックを行いたいリストのカテゴリを教えてください：")  # チェックするカテゴリを入力させる

# CSVファイルの読み込み
file_path = input("CSVファイルのパスを入力してください: ").strip('\"')
df = pd.read_csv(file_path)

# カテゴリ行を取得
category = df.iloc[category_row, start_column:].to_list()
category_str = '\t'.join(category)

# 1つ目のワークフローのAPI URLとユーザーIDの設定
api_url_1 = "https://api.dify.ai/v1/workflows/run"
api_key_1 = "app-ZM4ws6HzCGWBryy7JDjaRJOO"
user_id = "5bd1a47c-dd8d-4d1b-8905-5fceb8907e14"

# 2つ目のワークフローのAPI URLとキーの設定
api_url_2 = "https://api.dify.ai/v1/workflows/run"
api_key_2 = "app-RDmtSSE36hx27nDIBqSA9qhV"

# 出力用のリスト
results = []

# 処理する行数を取得
total_rows = last_row - first_row + 1

# 1つ目のワークフローを処理する関数（行ごとに処理）
def process_row(i):
    row_data = df.iloc[i, start_column:].to_list()
    row_str = '\t'.join(map(str, row_data))
    
    # APIへのリクエストデータの作成
    request_data_1 = {
        "inputs": {
            "row": row_str,
            "target": target_category,  # ユーザー入力されたカテゴリをtargetとして使用
            "category": category_str
        },
        "sys.files": [],
        "user": user_id  # user パラメータを追加
    }

    # APIリクエストのヘッダーにAPIキーを追加（1つ目）
    headers_1 = {
        "Authorization": f"Bearer {api_key_1}",  # APIキーをAuthorizationヘッダーに追加
        "Content-Type": "application/json"
    }

    try:
        # 1つ目のワークフローAPIコール
        response_1 = requests.post(api_url_1, json=request_data_1, headers=headers_1)
        
        if response_1.status_code == 200:
            result_1 = response_1.json()
            outputs_text_1 = result_1['data']['outputs'].get('text', '')
            row_data.append(outputs_text_1)
        else:
            row_data.append(f"Error {response_1.status_code}: {response_1.text}")
    except requests.exceptions.RequestException as e:
        row_data.append(f"Request Exception: {e}")

    return row_data

# 1つ目のワークフローを並列処理
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(process_row, i): i for i in range(first_row, last_row + 1)}

    with tqdm(total=total_rows, desc="Processing rows", unit="row") as pbar:
        for future in as_completed(futures):
            results.append(future.result())
            pbar.update(1)

# 最初の行にカテゴリ情報を追加
category.append("outputs_text_1")

# 1つ目のワークフローの結果をDataFrameに変換
results_df = pd.DataFrame(results, columns=category)

# 2つ目のワークフローを処理する関数（列ごとに処理）
def process_column(col_data, col_name):
    col_str = '\t'.join(map(str, col_data))
    category_data = df.iloc[category_row, start_column]  # 調査項目をcategoryとして使用

    # APIへのリクエストデータの作成
    request_data_2 = {
        "inputs": {
            "column": col_str,  # columnとして列データを送信
            "target": target_category,  # 1つ目のワークフローと同じtargetを使用
            "category": category_data  # 調査項目をcategoryとして送信
        },
        "sys.files": [],
        "user": user_id
    }

    headers_2 = {
        "Authorization": f"Bearer {api_key_2}",  # 2つ目のAPIキーをAuthorizationヘッダーに追加
        "Content-Type": "application/json"
    }

    try:
        # 2つ目のワークフローAPIコール
        response_2 = requests.post(api_url_2, json=request_data_2, headers=headers_2)
        
        if response_2.status_code == 200:
            result_2 = response_2.json()
            outputs_text_2 = result_2['data']['outputs'].get('text', '')
            return outputs_text_2
        else:
            return f"Error {response_2.status_code}: {response_2.text}"
    except requests.exceptions.RequestException as e:
        return f"Request Exception: {e}"

# 列ごとに処理を実行し、結果を最下行に追加
for i, col_name in enumerate(category[:-1]):  # outputs_text_1以外の列に対して処理を行う
    col_data = df.iloc[first_row:last_row + 1, start_column + i].to_list()
    output_2 = process_column(col_data, col_name)
    results_df.loc[last_row + 1, col_name] = output_2  # 結果を最下行に追加

# 最終結果をoutput.csvに保存
output_csv_path = file_path.replace('.csv', '_output.csv')
results_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')

print(f"最終結果が {output_csv_path} に保存されました。")