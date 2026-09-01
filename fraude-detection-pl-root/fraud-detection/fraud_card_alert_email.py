from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ============================================================================
# CARD FRAUD EMAIL ALERT SYSTEM
# ============================================================================

# Get Gmail credentials (outside handler for serialization compatibility)
EMAIL = "binary.wizard.leo@gmail.com"
APP_PASSWORD = dbutils.secrets.get(scope="fraud-detection", key="gmail_api_key")


def send_card_fraud_alert_email(fraud_row):
    """
    Send email alert for a card detected on the fraud watchlist.
    """
    try:
        # Extract fraud alert details
        customer_name = fraud_row.customer_full_name or "Valued Customer"
        customer_email = fraud_row.email
        transaction_id = fraud_row.transaction_id
        card_number = fraud_row.card_number
        amount = fraud_row.amount
        currency = fraud_row.currency
        merchant = fraud_row.merchant_name
        transaction_time = fraud_row.transaction_timestamp
        transaction_city = fraud_row.transaction_city
        transaction_country = fraud_row.transaction_country
        
        # Watchlist details
        risk_level = fraud_row.risk_level
        reason_code = fraud_row.reason_code
        reason_description = fraud_row.reason_description
        watchlist_action = fraud_row.watchlist_action
        
        # Calculated metrics
        amount_over_limit = fraud_row.amount_over_limit
        percent_over_limit = fraud_row.percent_over_limit
        
        # Mask card number for security (show last 4 digits)
        masked_card = f"****-****-****-{str(card_number)[-4:]}"
        
        # Create email subject based on risk level
        risk_emoji = {
            'HIGH': '🔴',
            'CRITICAL': '⛔',
            'MEDIUM': '🟡',
            'LOW': '🟢'
        }.get(risk_level, '⚠️')
        
        subject = f"{risk_emoji} URGENT: Fraud Alert - Card {masked_card} [{risk_level} Risk]"
        
        # Create HTML email body
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            
            <div style="background-color: #d32f2f; color: white; padding: 20px; text-align: center;">
                <h2 style="margin: 0;">{risk_emoji} FRAUD ALERT - IMMEDIATE ACTION REQUIRED</h2>
            </div>
            
            <div style="padding: 20px; background-color: #f8f9fa;">
                <p>Dear {customer_name},</p>
                
                <p style="font-size: 16px; color: #d32f2f; font-weight: bold;">
                    Your card has been flagged in our fraud watchlist system and a suspicious transaction 
                    was attempted. We have taken immediate action to protect your account.
                </p>
                
                <div style="background-color: #ffebee; padding: 15px; border-left: 4px solid #d32f2f; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #d32f2f;">🚨 Fraud Alert Details</h3>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr style="background-color: #ffcdd2;">
                            <td style="padding: 8px; font-weight: bold; width: 40%;">Risk Level:</td>
                            <td style="padding: 8px; color: #d32f2f; font-weight: bold; font-size: 18px;">
                                {risk_level}
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; font-weight: bold;">Card Number:</td>
                            <td style="padding: 8px;">{masked_card}</td>
                        </tr>
                        <tr style="background-color: #ffebee;">
                            <td style="padding: 8px; font-weight: bold;">Reason:</td>
                            <td style="padding: 8px;">{reason_description}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; font-weight: bold;">Reason Code:</td>
                            <td style="padding: 8px;">{reason_code}</td>
                        </tr>
                        <tr style="background-color: #ffebee;">
                            <td style="padding: 8px; font-weight: bold;">Action Taken:</td>
                            <td style="padding: 8px; font-weight: bold;">{watchlist_action}</td>
                        </tr>
                    </table>
                </div>
                
                <div style="background-color: white; padding: 15px; border-left: 4px solid #ff9800; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #ff9800;">Transaction Details</h3>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px; font-weight: bold; width: 40%;">Transaction ID:</td>
                            <td style="padding: 8px;">{transaction_id}</td>
                        </tr>
                        <tr style="background-color: #f8f9fa;">
                            <td style="padding: 8px; font-weight: bold;">Amount:</td>
                            <td style="padding: 8px; color: #d32f2f; font-weight: bold; font-size: 16px;">
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
                    </table>
                </div>
                
                <div style="background-color: #fff3e0; padding: 15px; border-left: 4px solid #ff9800; margin: 20px 0;">
                    <h4 style="margin-top: 0; color: #e65100;">⚠️ Limit Exceeded</h4>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px; font-weight: bold; width: 40%;">Over Limit By:</td>
                            <td style="padding: 8px; color: #d32f2f; font-weight: bold;">
                                {currency} {amount_over_limit:,.2f}
                            </td>
                        </tr>
                        <tr style="background-color: #fff3e0;">
                            <td style="padding: 8px; font-weight: bold;">Percent Over:</td>
                            <td style="padding: 8px; color: #d32f2f; font-weight: bold;">
                                {percent_over_limit:.1f}%
                            </td>
                        </tr>
                    </table>
                </div>
                
                <div style="background-color: #d32f2f; color: white; padding: 15px; border-radius: 5px; margin: 20px 0;">
                    <h4 style="margin-top: 0;">🔒 IMMEDIATE ACTION REQUIRED</h4>
                    <ol style="margin: 10px 0; padding-left: 20px;">
                        <li><strong>Call us immediately:</strong> 1-800-FRAUD-911</li>
                        <li><strong>Verify your identity</strong> and recent transactions</li>
                        <li><strong>Block your card</strong> if you did not authorize this transaction</li>
                        <li><strong>Request a new card</strong> with a new number</li>
                    </ol>
                    <p style="margin-bottom: 0; font-size: 14px;">
                        Our fraud team is standing by 24/7 to assist you.
                    </p>
                </div>
                
                <div style="background-color: #e3f2fd; padding: 15px; border-left: 4px solid #2196f3; margin: 20px 0;">
                    <h4 style="margin-top: 0; color: #1565c0;">ℹ️ What Happens Next</h4>
                    <p style="margin: 0;">
                        Your card has been temporarily {watchlist_action.lower()}d. No further transactions can be 
                        processed until you contact us to verify your identity. This is for your protection.
                    </p>
                </div>
                
                <p style="margin-top: 30px;">
                    Thank you for your immediate attention to this matter.<br>
                    <strong>FinGuard Fraud Detection & Prevention Team</strong><br>
                    <span style="font-size: 12px; color: #666;">Protecting your financial security 24/7</span>
                </p>
            </div>
            
            <div style="background-color: #343a40; color: white; padding: 15px; text-align: center; font-size: 12px;">
                <p style="margin: 0;">
                    This is an automated fraud alert from FinGuard Security System.<br>
                    <strong>Do NOT ignore this message.</strong> Call 1-800-FRAUD-911 immediately.<br>
                    Alert generated at: {fraud_row.alert_timestamp}
                </p>
            </div>
            
        </body>
        </html>
        """
        
        # Create email message
        msg = MIMEMultipart()
        msg["From"] = EMAIL
        msg["To"] = customer_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))
        
        # Send email
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL, APP_PASSWORD)
            server.send_message(msg)
        
        return f"✅ Fraud alert email sent to {customer_email} for card {masked_card} (Transaction: {transaction_id})"
        
    except Exception as e:
        return f"❌ Error sending fraud alert for transaction {fraud_row.transaction_id}: {str(e)}"


@dp.foreach_batch_sink(name="card_fraud_alert_email_sink")
def send_card_fraud_alerts(df: DataFrame, batch_id: int):
    """
    ForEachBatch sink that sends fraud alert emails for each flagged card transaction in the micro-batch.
    """
    # Collect fraud alerts from the batch
    fraud_alerts = df.collect()
    
    print(f"\n{'='*80}")
    print(f"Processing batch {batch_id}: {len(fraud_alerts)} card fraud alert(s) detected")
    print(f"{'='*80}\n")
    
    # Send email for each fraud alert
    for idx, alert in enumerate(fraud_alerts, 1):
        print(f"[{idx}/{len(fraud_alerts)}] Sending fraud alert for transaction: {alert.transaction_id}")
        print(f"    Card: ****-****-****-{str(alert.card_number)[-4:]}")
        print(f"    Risk Level: {alert.risk_level}")
        print(f"    Reason: {alert.reason_description}")
        result = send_card_fraud_alert_email(alert)
        print(f"    {result}")
    
    print(f"\n{'='*80}")
    print(f"Batch {batch_id} processing complete: {len(fraud_alerts)} fraud alert email(s) sent")
    print(f"{'='*80}\n")


@dp.append_flow(target="card_fraud_alert_email_sink")
def card_fraud_alert_stream():
    """
    Stream card fraud alerts to the email sink.
    Reads from the fraud_card_alert table and sends each alert to the email sink.
    """
    return spark.readStream.table("fraud_detection.gold.fraud_card_alert")
