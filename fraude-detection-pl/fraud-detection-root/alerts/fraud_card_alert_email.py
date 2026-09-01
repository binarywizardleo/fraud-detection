"""
Alert Layer: Fraud Card Alert Email Notifications
================================================
Sends email alerts for transactions involving cards on the fraud watchlist.
Reads from the gold fraud_card_alerts table and sends an email per alert.

Dependencies:
- fraud_detection.gold.fraud_card_alerts
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Gmail credentials (outside handler for serialization compatibility)
EMAIL = "binary.wizard.leo@gmail.com"
APP_PASSWORD = dbutils.secrets.get(scope="fraud-detection", key="gmail_api_key")


def send_fraud_card_alert_email(transaction_row):
    """
    Send email alert for a single fraud card transaction.
    """
    try:
        customer_name = transaction_row.customer_full_name or "Valued Customer"
        customer_email = transaction_row.customer_email
        amount = transaction_row.amount
        currency = transaction_row.currency
        merchant = transaction_row.merchant_name
        transaction_time = transaction_row.transaction_timestamp
        transaction_id = transaction_row.transaction_id
        amount_over_limit = transaction_row.amount_over_limit
        percent_over_limit = transaction_row.percent_over_limit
        transaction_city = transaction_row.transaction_city
        transaction_country = transaction_row.transaction_country
        watchlist_risk_level = transaction_row.watchlist_risk_level
        watchlist_reason = transaction_row.watchlist_reason_description
        watchlist_action = transaction_row.watchlist_action

        subject = f"\U0001f6a8 Fraud Card Alert - {currency} {amount:,.2f}"

        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            
            <div style="background-color: #dc3545; color: white; padding: 20px; text-align: center;">
                <h2 style="margin: 0;">\U0001f6a8 Fraud Card Alert</h2>
            </div>
            
            <div style="padding: 20px; background-color: #f8f9fa;">
                <p>Dear {customer_name},</p>
                
                <p style="font-size: 16px; color: #dc3545; font-weight: bold;">
                    We detected a transaction on a card that is on our fraud watchlist.
                </p>
                
                <div style="background-color: white; padding: 15px; border-left: 4px solid #dc3545; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #dc3545;">Transaction Details</h3>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px; font-weight: bold; width: 40%;">Transaction ID:</td>
                            <td style="padding: 8px;">{transaction_id}</td>
                        </tr>
                        <tr style="background-color: #f8f9fa;">
                            <td style="padding: 8px; font-weight: bold;">Amount:</td>
                            <td style="padding: 8px; color: #dc3545; font-weight: bold; font-size: 18px;">
                                {currency} {amount:,.2f}
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; font-weight: bold;">Merchant:</td>
                            <td style="padding: 8px;">{merchant}</td>
                        </tr>
                        <tr style="background-color: #f8f9fa;">
                            <td style="padding: 8px; font-weight: bold;">Location:</td>
                            <td style="padding: 8px;">{transaction_city}, {transaction_country}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; font-weight: bold;">Date & Time:</td>
                            <td style="padding: 8px;">{transaction_time}</td>
                        </tr>
                        <tr style="background-color: #f8f9fa;">
                            <td style="padding: 8px; font-weight: bold;">Over Limit By:</td>
                            <td style="padding: 8px; color: #dc3545; font-weight: bold;">
                                {currency} {amount_over_limit:,.2f} ({percent_over_limit:.1f}%)
                            </td>
                        </tr>
                    </table>
                </div>
                
                <div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0;">
                    <h4 style="margin-top: 0; color: #856404;">\U0001f50d Watchlist Information</h4>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px; font-weight: bold; width: 40%;">Risk Level:</td>
                            <td style="padding: 8px; color: #dc3545; font-weight: bold;">{watchlist_risk_level}</td>
                        </tr>
                        <tr style="background-color: #f8f9fa;">
                            <td style="padding: 8px; font-weight: bold;">Reason:</td>
                            <td style="padding: 8px;">{watchlist_reason}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; font-weight: bold;">Action:</td>
                            <td style="padding: 8px;">{watchlist_action}</td>
                        </tr>
                    </table>
                </div>
                
                <div style="background-color: #f8d7da; padding: 15px; border-left: 4px solid #dc3545; margin: 20px 0;">
                    <h4 style="margin-top: 0; color: #721c24;">\u26a0\ufe0f Immediate Action Required</h4>
                    <p style="margin: 0;">
                        Your card has been flagged on our fraud watchlist. If you did not authorize this
                        transaction, please contact our fraud department immediately at
                        <strong>1-800-FRAUD-HELP</strong> or reply to this email. Your card may be
                        temporarily blocked for your protection.
                    </p>
                </div>
                
                <p style="margin-top: 30px;">
                    Best regards,<br>
                    <strong>FinGuard Fraud Detection Team</strong><br>
                    <span style="font-size: 12px; color: #666;">Available 24/7 for your security</span>
                </p>
            </div>
            
            <div style="background-color: #343a40; color: white; padding: 15px; text-align: center; font-size: 12px;">
                <p style="margin: 0;">
                    This is an automated alert from FinGuard Fraud Detection System.<br>
                    Please do not reply to this email for general inquiries.
                </p>
            </div>
            
        </body>
        </html>
        """

        msg = MIMEMultipart()
        msg["From"] = EMAIL
        msg["To"] = customer_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL, APP_PASSWORD)
            server.send_message(msg)
        
        return f"\u2705 Email sent to {customer_email} for transaction {transaction_id}"
    
    except Exception as e:
        return f"\u274c Error sending email for transaction {transaction_row.transaction_id}: {str(e)}"


@dp.foreach_batch_sink(name="fraud_card_alert_email_sink")
def send_fraud_card_alerts(df: DataFrame, batch_id: int):
    """
    ForEachBatch sink that sends email alerts for each fraud card alert in the micro-batch.
    """
    transactions = df.collect()
    
    print(f"\n{'='*80}")
    print(f"Processing batch {batch_id}: {len(transactions)} fraud card alert(s) detected")
    print(f"{'='*80}\n")
    
    for idx, transaction in enumerate(transactions, 1):
        print(f"[{idx}/{len(transactions)}] Sending alert for transaction: {transaction.transaction_id}")
        result = send_fraud_card_alert_email(transaction)
        print(f"    {result}")
    
    print(f"\n{'='*80}")
    print(f"Batch {batch_id} processing complete: {len(transactions)} email(s) sent")
    print(f"{'='*80}\n")


@dp.append_flow(target="fraud_card_alert_email_sink")
def fraud_card_alert_stream():
    """
    Stream fraud card alerts to the email sink.
    Reads from the fraud_card_alerts gold table and sends each alert to the email sink.
    """
    return spark.readStream.table("fraud_detection.gold.fraud_card_alerts")
