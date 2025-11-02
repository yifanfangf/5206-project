import mysql.connector
import csv
from datetime import datetime
import os

# --- 数据库连接信息 ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '781013',
    'database': 'olist_db'
}

SQL_QUERIES = [
    {
        "name": "DAU_Calculation",
        "query": """
            SELECT
                DATE_FORMAT(order_approved_at,'%Y年-%m月-%d日') AS Datetime,
                COUNT(DISTINCT customer_id) AS DAU
            FROM orders
            GROUP BY Datetime
            ORDER BY Datetime;
        """
    },
    {
        "name": "MAU_Calculation",
        "query": """
            SELECT
                DATE_FORMAT(order_approved_at,'%Y年-%m月') AS Datetime,
                COUNT(DISTINCT customer_id) AS MAU -- 修正为 MAU
            FROM orders
            GROUP BY Datetime
            ORDER BY Datetime;
        """
    },
    {
        "name": "Customer_Distribution",
        "query": """
            SELECT
                customer_state  AS 州,
                COUNT(customer_id) AS 客户量
            FROM customers
            GROUP BY customer_state;
        """
    },
    {
    "name": "Payment_Statistics",
    "query": """
                    SELECT
                        payment_type AS 支付方式,
                        COUNT(order_id) AS 订单总量,
                        ROUND(COUNT(order_id)/SUM(COUNT(order_id)) OVER(),2) AS 支付方式占比,
                        ROUND(AVG(payment_value),2) AS 平均消费
                    FROM order_payments
                    GROUP BY payment_type
                    ORDER BY 订单总量 DESC;
                """
    },
    {
        "name": "Installments_Analysis",
        "query": """
            SELECT
                payment_installments AS 分期数量,
                COUNT(DISTINCT order_id) AS 订单数量
            FROM order_payments
            WHERE payment_type = 'credit_card'
            GROUP BY payment_installments;
        """
    },
    {
        "name": "Order_Time_Distribution",
        "query": """
            SELECT
                DATE_FORMAT(order_purchase_timestamp,'%H时') AS Hour,
                COUNT(order_id) AS 订单数量
            FROM orders
            GROUP BY Hour
            ORDER BY Hour;
        """
    },
    {
        "name": "Daily_GMV",
        "query": """
            SELECT
                DATE_FORMAT(od.order_approved_at,'%Y-%m-%d') AS datetime,
                ROUND(SUM(py.payment_value),2) AS GMV
            FROM order_payments AS py
            JOIN orders AS od ON py.order_id = od.order_id
            WHERE od.order_status = 'delivered'
            GROUP BY datetime
            ORDER BY datetime;
        """
    },
    {
        "name": "GMV_By_State",
        "query": """
            SELECT
                c.customer_state  AS state,
                ROUND(SUM(py.payment_value), 2)  AS GMV
            FROM orders AS o
            JOIN customers AS c ON o.customer_id = c.customer_id
            JOIN order_payments AS py ON o.order_id = py.order_id
            WHERE o.order_status = 'delivered'
            GROUP BY c.customer_state
            ORDER BY GMV DESC;
        """
    },
    {
        "name": "Average_Order_Value",
        "query": """
            SELECT
                ROUND(AVG(py.payment_value),2) AS 订单均价
            FROM order_payments AS py
            JOIN orders AS od ON py.order_id = od.order_id
            WHERE od.order_status = 'delivered';
        """
    },
    {
        "name": "Monthly_Order_Count",
        "query": """
            SELECT
                DATE_FORMAT(order_approved_at,'%Y年-%m月') AS Datetime,
                COUNT(order_id) AS 订单数量
            FROM orders
            WHERE order_status = 'delivered'
            GROUP BY Datetime;
        """
    },
    {
        "name": "Daily_Delivery_Completion_Rate",
        "query": {
            "setup": [
                "ALTER TABLE orders ADD COLUMN Delivery_completion INT;",
                "UPDATE orders SET Delivery_completion = 1 WHERE order_status = 'delivered';",
                "UPDATE orders SET Delivery_completion = 0 WHERE order_status != 'delivered';"
            ],
            "select": """
                SELECT
                    DATE_FORMAT(order_approved_at,'%Y-%m-%d') AS Datetime,
                    ROUND(SUM(Delivery_completion)/COUNT(order_id),2) AS 交付完成率
                FROM orders
                GROUP BY Datetime
                ORDER BY Datetime;
            """
        }
    },
    {
        "name": "Monthly_ARPU_Calculation",
        "query": """
            SELECT
                DATE_FORMAT(od.order_approved_at,'%Y年-%m月') AS Datetime,
                ROUND(SUM(pd.payment_value)/COUNT(DISTINCT cd.customer_id),2) AS ARPU值
            FROM order_payments AS pd
            JOIN orders AS od ON pd.order_id = od.order_id
            JOIN customers AS cd ON cd.customer_id = od.customer_id
            WHERE od.order_status = 'delivered'
            GROUP BY Datetime;
        """
    }

]


def save_to_csv(filename, columns, data):
    """将数据保存到 CSV 文件"""
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_path = os.path.join(output_dir, f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

    try:
        with open(file_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            csv_writer = csv.writer(csvfile)

            csv_writer.writerow(columns)

            csv_writer.writerows(data)

        print(f"✅ 结果已成功保存到: {file_path} ")
    except IOError as e:
        print(f"❌ 写入文件失败: {e}")


def run_sql_query(config, sql_list):
    """连接数据库并运行所有 SQL 查询，支持混合操作"""
    conn = None
    try:
        print("尝试连接数据库...")
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        print("数据库连接成功！")

        for sql_item in sql_list:
            query_name = sql_item['name']
            query_content = sql_item['query']
            print("-" * 50)
            print(f"执行任务: {query_name}")

            # --- 关键逻辑修改：处理混合操作 ---
            if isinstance(query_content, dict) and "setup" in query_content:
                for setup_sql in query_content["setup"]:
                    try:
                        print(f"  -> 执行设置语句: {setup_sql[:50]}...")
                        cursor.execute(setup_sql)
                    except mysql.connector.Error as err:
                        if "Duplicate column name" in str(err):
                            print("  -> 警告: Delivery_completion 列已存在，跳过 ALTER。")
                        else:
                            raise err

                conn.commit()
                print("  -> 数据库更改已提交。")

                final_query = query_content["select"]
            else:
                final_query = query_content

            # 执行最终查询
            cursor.execute(final_query)
            results = cursor.fetchall()

            if results:
                columns = [i[0] for i in cursor.description]
                print(f"列名: {columns}")
                print(f"共找到 {len(results)} 行结果。")

                # 保存到 CSV 文件
                save_to_csv(query_name, columns, results)
            else:
                print("未找到结果。")

    except mysql.connector.Error as err:
        print(f"❌ 数据库操作失败: {err}")

    finally:
        # 关闭连接
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("\n数据库连接已关闭。")


if __name__ == '__main__':
    run_sql_query(DB_CONFIG, SQL_QUERIES)