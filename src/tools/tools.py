# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

import json
from typing import Any

import requests
from opensearch.helper import get_index_mapping, get_shards, list_indices, search_index
from pydantic import BaseModel

# 都道府県 → 気象庁 overview_forecast API 用コード
AREA_CODES = {
    "北海道": "016000",
    "青森県": "020000",
    "岩手県": "030000",
    "宮城県": "040000",
    "秋田県": "050000",
    "山形県": "060000",
    "福島県": "070000",
    "茨城県": "080000",
    "栃木県": "090000",
    "群馬県": "100000",
    "埼玉県": "110000",
    "千葉県": "120000",
    "東京都": "130000",
    "神奈川県": "140000",
    "新潟県": "150000",
    "富山県": "160000",
    "石川県": "170000",
    "福井県": "180000",
    "山梨県": "190000",
    "長野県": "200000",
    "岐阜県": "210000",
    "静岡県": "220000",
    "愛知県": "230000",
    "三重県": "240000",
    "滋賀県": "250000",
    "京都府": "260000",
    "大阪府": "270000",
    "兵庫県": "280000",
    "奈良県": "290000",
    "和歌山県": "300000",
    "鳥取県": "310000",
    "島根県": "320000",
    "岡山県": "330000",
    "広島県": "340000",
    "山口県": "350000",
    "徳島県": "360000",
    "香川県": "370000",
    "愛媛県": "380000",
    "高知県": "390000",
    "福岡県": "400000",
    "佐賀県": "410000",
    "長崎県": "420000",
    "熊本県": "430000",
    "大分県": "440000",
    "宮崎県": "450000",
    "鹿児島県": "460100",
    "沖縄県": "471000",
}


class ListIndicesArgs(BaseModel):
    pass  # no args needed


class GetIndexMappingArgs(BaseModel):
    index: str


class SearchIndexArgs(BaseModel):
    index: str
    query: Any


class GetShardsArgs(BaseModel):
    index: str


class WeatherArgs(BaseModel):
    prefecture: str  # 都道府県名


async def get_weather_tool(args: WeatherArgs) -> list[dict]:
    try:
        # 都道府県コードを取得
        code = AREA_CODES.get(args.prefecture)
        if not code:
            return [
                {
                    "type": "text",
                    "text": f"⚠️ 「{args.prefecture}」は対応していません。47都道府県のいずれかを指定してください。",
                }
            ]

        # 気象庁APIのURL
        url = f"https://www.jma.go.jp/bosai/forecast/data/overview_forecast/{code}.json"

        # APIリクエスト
        response = requests.get(url)
        response.raise_for_status()  # HTTPエラーをチェック

        # レスポンスをJSON形式で取得
        data = response.json()

        # 必要な情報を抽出
        office = data.get("publishingOffice", "不明")
        datetime = data.get("reportDatetime", "不明")
        text = data.get("text", "天気情報が取得できませんでした。")

        return [
            {
                "type": "text",
                "text": f"📍 {args.prefecture}（{office} 発表）\n🕒 {datetime}\n\n{text}",
            }
        ]
    except Exception as e:
        return [
            {
                "type": "text",
                "text": "⚠️ 天気情報の取得に失敗しました（気象庁API通信エラー）",
            }
        ]


async def list_indices_tool(args: ListIndicesArgs) -> list[dict]:
    try:
        indices = list_indices()
        indices_text = "\n".join(index["index"] for index in indices)

        # Return in MCP expected format
        return [{"type": "text", "text": indices_text}]
    except Exception as e:
        return [{"type": "text", "text": f"Error listing indices: {str(e)}"}]


async def get_index_mapping_tool(args: GetIndexMappingArgs) -> list[dict]:
    try:
        mapping = get_index_mapping(args.index)
        formatted_mapping = json.dumps(mapping, indent=2)

        return [
            {"type": "text", "text": f"Mapping for {args.index}:\n{formatted_mapping}"}
        ]
    except Exception as e:
        return [{"type": "text", "text": f"Error getting mapping: {str(e)}"}]


async def search_index_tool(args: SearchIndexArgs) -> list[dict]:
    try:
        result = search_index(args.index, args.query)

        # embedding フィールドと metadata 内の _node_content を除外
        for item in result.get("hits", {}).get("hits", []):
            if isinstance(item, dict) and "_source" in item:
                # embedding フィールドを削除
                if "embedding" in item["_source"]:
                    del item["_source"]["embedding"]

                # metadata 内の _node_content を削除
                if "metadata" in item["_source"] and isinstance(
                    item["_source"]["metadata"], dict
                ):
                    if "_node_content" in item["_source"]["metadata"]:
                        del item["_source"]["metadata"]["_node_content"]

        formatted_result = json.dumps(result, indent=2)

        return [
            {
                "type": "text",
                "text": f"Original Search results from {args.index}:\n{formatted_result}",
            }
        ]
    except Exception as e:
        return [{"type": "text", "text": f"Error searching index: {str(e)}"}]


async def get_shards_tool(args: GetShardsArgs) -> list[dict]:
    try:
        result = get_shards(args.index)

        if isinstance(result, dict) and "error" in result:
            return [
                {"type": "text", "text": f"Error getting shards: {result['error']}"}
            ]
        formatted_text = "index | shard | prirep | state | docs | store | ip | node\n"

        # Format each shard row
        for shard in result:
            formatted_text += f"{shard['index']} | "
            formatted_text += f"{shard['shard']} | "
            formatted_text += f"{shard['prirep']} | "
            formatted_text += f"{shard['state']} | "
            formatted_text += f"{shard['docs']} | "
            formatted_text += f"{shard['store']} | "
            formatted_text += f"{shard['ip']} | "
            formatted_text += f"{shard['node']}\n"

        return [{"type": "text", "text": formatted_text}]
    except Exception as e:
        return [{"type": "text", "text": f"Error getting shards information: {str(e)}"}]


TOOL_REGISTRY = {
    "ListIndexTool": {
        "description": "Lists all indices in OpenSearch",
        "input_schema": ListIndicesArgs.model_json_schema(),
        "function": list_indices_tool,
        "args_model": ListIndicesArgs,
    },
    "IndexMappingTool": {
        "description": "Retrieves index mapping and setting information for an index in OpenSearch",
        "input_schema": GetIndexMappingArgs.model_json_schema(),
        "function": get_index_mapping_tool,
        "args_model": GetIndexMappingArgs,
    },
    "SearchIndexTool": {
        "description": "Searches an index using a query written in query domain-specific language (DSL) in OpenSearch",
        "input_schema": SearchIndexArgs.model_json_schema(),
        "function": search_index_tool,
        "args_model": SearchIndexArgs,
    },
    "GetShardsTool": {
        "description": "Gets information about shards in OpenSearch",
        "input_schema": GetShardsArgs.model_json_schema(),
        "function": get_shards_tool,
        "args_model": GetShardsArgs,
    },
    "GetWeatherTool": {
        "description": "Retrieves weather information for a Japanese prefecture using the Japan Meteorological Agency API",
        "input_schema": WeatherArgs.model_json_schema(),
        "function": get_weather_tool,
        "args_model": WeatherArgs,
    },
}
