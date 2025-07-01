import threading
import json
import requests
import mysql.connector
from datetime import datetime, timezone
from uuid import uuid4
import re

# ---------- 配置读取 ----------
with open('config.json', 'r') as f:
    CONFIG = json.load(f)

DB_CONFIG = CONFIG['mysql']

XERO_TOKEN_URL = 'https://identity.xero.com/connect/token'
XERO_API_URL = 'https://api.xero.com/api.xro/2.0/BankTransactions'
XERO_CONNECTIONS_URL = 'https://api.xero.com/connections'


# ---------- 数据库连接 ----------
def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


# ---------- 日期解析函数 ----------
def parse_xero_date(date_string):
    """解析Xero的日期格式"""
    if not date_string:
        return None
    
    # 处理ISO格式日期: "2025-03-29T00:00:00"
    if 'T' in date_string:
        return datetime.fromisoformat(date_string.replace('T', ' ')).date()
    
    # 处理时间戳格式: "/Date(1743206400000+0000)/"
    if date_string.startswith('/Date(') and date_string.endswith(')/'):
        timestamp_match = re.search(r'/Date\((\d+)', date_string)
        if timestamp_match:
            timestamp = int(timestamp_match.group(1)) / 1000  # 转换为秒
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).date()
    
    return None


# ---------- 计算实际交易金额 ----------
def calculate_transaction_amount(transaction_data):
    """根据交易类型计算正确的金额（正数=收入，负数=支出）"""
    total = transaction_data.get('Total', 0)
    transaction_type = transaction_data.get('Type', '')
    
    # SPEND = 支出（负数），RECEIVE = 收入（正数）
    if transaction_type == 'SPEND':
        return -abs(total)  # 确保是负数
    elif transaction_type == 'RECEIVE':
        return abs(total)   # 确保是正数
    else:
        return total


# ---------- 获取所有租户 ----------
def get_all_active_tenants():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, tenant_code FROM tenants WHERE status = 'ACTIVE'")
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


# ---------- 获取租户下所有客户 ----------
def get_customers_by_tenant(tenant_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT c.id as customer_id, c.tenant_id, s.api_credentials
        FROM customers c
        JOIN customer_accounting_settings s ON c.id = s.customer_id
        WHERE c.tenant_id = %s
    """, (tenant_id,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


# ---------- 刷新 Token ----------
def refresh_access_token(api_credentials):
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': api_credentials['refresh_token'],
        'client_id': api_credentials['client_id'],
        'client_secret': api_credentials['client_secret'],
    }
    response = requests.post(XERO_TOKEN_URL, data=data)
    response.raise_for_status()
    token_data = response.json()
    return token_data['access_token'], token_data['refresh_token']


# ---------- 获取租户 ID ----------
def get_xero_tenant_id(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(XERO_CONNECTIONS_URL, headers=headers)
    response.raise_for_status()
    connections = response.json()
    if not connections:
        raise Exception('❌ No Xero tenant connections found')
    return connections[0]['tenantId']


# ---------- 获取交易记录 ----------
def fetch_xero_transactions(access_token, tenant_id):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'xero-tenant-id': tenant_id,
        'Accept': 'application/json'
    }
    response = requests.get(XERO_API_URL, headers=headers)
    response.raise_for_status()
    return response.json().get('BankTransactions', [])


# ---------- 写入数据库 ----------
def insert_transaction_record(cursor, txn, tenant_id, customer_id):
    """插入交易记录到数据库"""
    bank_account = txn.get('BankAccount', {})
    contact = txn.get('Contact', {})
    
    # 解析交易日期
    transaction_date = parse_xero_date(txn.get('DateString'))
    
    # 计算正确的交易金额
    amount = calculate_transaction_amount(txn)
    
    cursor.execute("""
        INSERT IGNORE INTO bank_transactions (
            id, tenant_id, customer_id, transaction_id, 
            bank_account_id, bank_account_code, bank_account_name,
            transaction_date, amount, sub_total, total_tax, total, currency_code,
            description, reference, transaction_type, status, line_amount_types,
            contact_id, contact_name,
            is_reconciled, has_attachments,
            raw_date_string, raw_date_timestamp, updated_date_utc,
            imported_from, reconciliation_status, created_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s
        )
    """, (
        str(uuid4()), tenant_id, customer_id, txn.get('BankTransactionID'),
        bank_account.get('AccountID'), bank_account.get('Code'), bank_account.get('Name'),
        transaction_date, amount, txn.get('SubTotal'), txn.get('TotalTax'), txn.get('Total'), txn.get('CurrencyCode', 'AUD'),
        txn.get('Narration', ''), txn.get('Reference', ''), txn.get('Type'), txn.get('Status'), txn.get('LineAmountTypes'),
        contact.get('ContactID'), contact.get('Name'),
        txn.get('IsReconciled', False), txn.get('HasAttachments', False),
        txn.get('DateString'), txn.get('Date'), txn.get('UpdatedDateUTC'),
        'XERO', 'UNMATCHED', datetime.now(timezone.utc)
    ))


# ---------- 更新 refresh_token 到数据库 ----------
def update_refresh_token_in_db(customer_id, new_refresh_token):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE customer_accounting_settings
        SET api_credentials = JSON_SET(api_credentials, '$.refresh_token', %s)
        WHERE customer_id = %s
    """, (new_refresh_token, customer_id))
    conn.commit()
    cursor.close()
    conn.close()


# ---------- 主处理函数 ----------
def process_tenant_transactions(tenant_id, tenant_code):
    print(f"[{tenant_code}] ▶️ Thread started")
    customers = get_customers_by_tenant(tenant_id)
    
    if not customers:
        print(f"[{tenant_code}] ⚠️ No customers found for tenant")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    total_transactions = 0

    for customer in customers:
        try:
            creds = json.loads(customer['api_credentials'])

            # Step 1: 刷新 access_token
            access_token, new_refresh_token = refresh_access_token(creds)

            # Step 2: 获取 tenant_id
            tenant_xero_id = get_xero_tenant_id(access_token)

            # Step 3: 获取银行交易记录
            transactions = fetch_xero_transactions(access_token, tenant_xero_id)
            print(f"[{tenant_code}] ✅ Customer {customer['customer_id']} - {len(transactions)} transactions fetched")

            # Step 4: 处理每个交易记录
            inserted_count = 0
            for txn in transactions:
                try:
                    insert_transaction_record(cursor, txn, tenant_id, customer['customer_id'])
                    inserted_count += 1
                except mysql.connector.Error as db_error:
                    if db_error.errno == 1062:  # 重复键错误
                        continue  # 跳过重复记录
                    else:
                        print(f"[{tenant_code}] ❌ Database error for transaction {txn.get('BankTransactionID')}: {db_error}")

            conn.commit()
            total_transactions += inserted_count
            print(f"[{tenant_code}] ✅ Customer {customer['customer_id']} - {inserted_count} transactions inserted")

            # Step 5: 更新 refresh_token（如果发生变化）
            if creds.get('refresh_token') != new_refresh_token:
                update_refresh_token_in_db(customer['customer_id'], new_refresh_token)
                print(f"[{tenant_code}] 🔄 Refresh token updated for customer {customer['customer_id']}")

        except requests.exceptions.RequestException as req_error:
            print(f"[{tenant_code}] ❌ API error with customer {customer['customer_id']}: {req_error}")
        except json.JSONDecodeError as json_error:
            print(f"[{tenant_code}] ❌ JSON parsing error with customer {customer['customer_id']}: {json_error}")
        except Exception as e:
            print(f"[{tenant_code}] ❌ Unexpected error with customer {customer['customer_id']}: {e}")

    cursor.close()
    conn.close()
    print(f"[{tenant_code}] ✅ Thread finished - Total {total_transactions} transactions processed")


# ---------- 主入口 ----------
def main():
    print(f"🚀 Starting Xero bank transactions sync at {datetime.now()}")
    
    tenants = get_all_active_tenants()
    if not tenants:
        print("⚠️ No active tenants found")
        return
    
    print(f"📋 Found {len(tenants)} active tenants")
    threads = []

    # 创建并启动线程
    for tenant in tenants:
        thread = threading.Thread(
            target=process_tenant_transactions,
            args=(tenant['id'], tenant['tenant_code']),
            name=f"Tenant-{tenant['tenant_code']}"
        )
        threads.append(thread)
        thread.start()

    # 等待所有线程完成
    for thread in threads:
        thread.join()

    print(f"✅ 所有租户同步完成 - {datetime.now()}")


if __name__ == '__main__':
    main()