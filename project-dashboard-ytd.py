#!/usr/bin/env python3
"""
Service Fusion API Data Extraction for GitHub Actions
Extracts Year-to-Date data and creates Power BI ready Excel files
"""

import requests
import json
import datetime
import pandas as pd
import time
import logging
import re
import os
import sys
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class ServiceFusionExtractor:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expires_at = None
        self.base_url = "https://api.servicefusion.com"
        self.session = requests.Session()
        
        self.endpoints = {
            'customers': '/v1/customers',
            'jobs': '/v1/jobs',
            'estimates': '/v1/estimates',
            'invoices': '/v1/invoices'
        }

    def authenticate(self) -> bool:
        """Authenticate with Service Fusion API"""
        try:
            if self.access_token and self.token_expires_at and datetime.datetime.now() < self.token_expires_at:
                return True
            
            logger.info("Authenticating with Service Fusion API...")
            
            auth_url = f"{self.base_url}/oauth/access_token"
            auth_data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
            
            response = requests.post(auth_url, data=auth_data, timeout=30)
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                
                expires_in = token_data.get('expires_in', 3600)
                try:
                    expires_in = int(expires_in)
                except (ValueError, TypeError):
                    expires_in = 3600
                    
                self.token_expires_at = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)
                
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                })
                
                logger.info("Authentication successful")
                return True
            else:
                logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False

    def extract_with_forced_sort(self, endpoint_name: str, date_range: str = "Year to Date") -> List[Dict]:
        """Extract data with forced sorting to get recent data first"""
        try:
            if not self.authenticate():
                return []
            
            url = f"{self.base_url}{self.endpoints[endpoint_name]}"
            all_records = []
            page = 1
            
            logger.info(f"Starting extraction for {endpoint_name} ({date_range})")
            
            # Calculate date range
            today = datetime.datetime.now()
            if date_range == "Year to Date":
                start_date = datetime.datetime(today.year, 1, 1)
            elif date_range == "Month to Date":
                start_date = datetime.datetime(today.year, today.month, 1)
            else:  # All Data
                start_date = datetime.datetime(2000, 1, 1)
            
            while True:
                # Force sort by creation date descending
                params = {'sort': '-created_at'}
                if page > 1:
                    params['page'] = page
                
                logger.info(f"Requesting {endpoint_name} page {page}")
                
                try:
                    response = self.session.get(url, params=params, timeout=60)
                    
                    if response.status_code != 200:
                        logger.error(f"HTTP Error {response.status_code} on page {page}")
                        if page == 1:  # Try without sort on first page
                            params = {'page': page} if page > 1 else {}
                            response = self.session.get(url, params=params, timeout=60)
                            if response.status_code != 200:
                                break
                        else:
                            break
                    
                    data = response.json()
                    records = data.get('items', [])
                    meta = data.get('_meta', {})
                    
                    if not records:
                        logger.info(f"No more records on page {page}")
                        break
                    
                    logger.info(f"Page {page}: Found {len(records)} records")
                    
                    # For YTD/MTD, stop when we hit old data
                    if date_range in ["Year to Date", "Month to Date"]:
                        current_year_records = []
                        old_data_count = 0
                        
                        for record in records:
                            date_val = record.get('created_at') or record.get('date')
                            if date_val:
                                try:
                                    if str(today.year) in str(date_val):
                                        current_year_records.append(record)
                                    else:
                                        old_data_count += 1
                                except:
                                    current_year_records.append(record)  # Include if uncertain
                            else:
                                current_year_records.append(record)  # Include if no date
                        
                        all_records.extend(current_year_records)
                        
                        # Stop if more than half the page is old data
                        if old_data_count > len(records) / 2 and page > 1:
                            logger.info(f"Stopping at page {page}: Found {old_data_count} old records")
                            break
                    else:
                        all_records.extend(records)
                    
                    # Pagination check
                    current_page = meta.get('currentPage', page)
                    page_count = meta.get('pageCount', 999)
                    
                    if current_page >= page_count:
                        logger.info(f"Reached last page ({current_page}/{page_count})")
                        break
                    
                    # Safety limits
                    if len(all_records) >= 50000:
                        logger.info(f"Reached safety limit of 50,000 records")
                        break
                        
                    if page > 500:
                        logger.info(f"Reached page limit {page}")
                        break
                    
                    page += 1
                    time.sleep(0.1)  # Be nice to the API
                    
                except requests.exceptions.Timeout:
                    logger.error(f"Timeout on page {page}")
                    break
                except Exception as e:
                    logger.error(f"Error on page {page}: {str(e)}")
                    break
            
            logger.info(f"Extracted {len(all_records)} records from {endpoint_name}")
            return all_records
            
        except Exception as e:
            logger.error(f"Error in extraction for {endpoint_name}: {str(e)}")
            return []

    def calculate_due_date(self, invoice_date: str) -> str:
        """Calculate due date as 15 days after invoice date"""
        if not invoice_date:
            return None
        
        try:
            date_str = str(invoice_date).strip()
            
            if 'T' in date_str:
                if 'Z' in date_str:
                    invoice_dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00')).replace(tzinfo=None)
                else:
                    invoice_dt = datetime.datetime.fromisoformat(date_str).replace(tzinfo=None)
            else:
                invoice_dt = datetime.datetime.strptime(date_str[:10], '%Y-%m-%d')
            
            due_dt = invoice_dt + datetime.timedelta(days=15)
            
            if 'T' in date_str:
                return due_dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            else:
                return due_dt.strftime('%Y-%m-%d')
                
        except Exception as e:
            logger.debug(f"Error calculating due date for '{invoice_date}': {e}")
            return None

    def create_powerbi_summary(self, all_data: Dict[str, List[Dict]]) -> pd.DataFrame:
        """Create Power BI ready summary"""
        try:
            logger.info("Creating Power BI summary...")
            
            jobs_data = all_data.get('jobs', [])
            invoices_data = all_data.get('invoices', [])
            estimates_data = all_data.get('estimates', [])
            
            logger.info(f"Input: {len(jobs_data)} jobs, {len(invoices_data)} invoices, {len(estimates_data)} estimates")
            
            # Use estimates as jobs if no jobs found
            if not jobs_data and estimates_data:
                logger.info("Using estimates as job data")
                jobs_data = estimates_data
                for estimate in jobs_data:
                    if not estimate.get('end_date'):
                        estimate['end_date'] = estimate.get('start_date')
            
            if not jobs_data:
                logger.warning("No jobs or estimates data available")
                return pd.DataFrame()
            
            # Create invoice lookup
            invoices_by_customer = {}
            for invoice in invoices_data:
                customer = invoice.get('customer', '')
                if customer:
                    if customer not in invoices_by_customer:
                        invoices_by_customer[customer] = []
                    invoices_by_customer[customer].append(invoice)
            
            summary_records = []
            processed_job_ids = set()
            
            for job in jobs_data:
                try:
                    job_id = job.get('id')
                    
                    # Skip duplicates
                    if job_id in processed_job_ids:
                        continue
                    
                    if job_id:
                        processed_job_ids.add(job_id)
                    
                    job_number = job.get('number')
                    job_name = job.get('description', 'No Description')
                    customer_id = job.get('customer_id')
                    customer_name = job.get('customer_name', 'Unknown')
                    
                    # Find matching invoice
                    related_invoice = None
                    invoice_match_method = "None"
                    customer_invoices = invoices_by_customer.get(customer_name, [])
                    
                    if customer_invoices:
                        job_total = job.get('total', 0) or 0
                        
                        # Try amount match first
                        if job_total > 0:
                            for invoice in customer_invoices:
                                invoice_total = invoice.get('total', 0) or 0
                                if invoice_total > 0:
                                    ratio = abs(invoice_total - job_total) / job_total
                                    if ratio <= 0.1:  # Within 10%
                                        related_invoice = invoice
                                        invoice_match_method = "Amount Match"
                                        break
                        
                        # Fall back to most recent
                        if not related_invoice and customer_invoices:
                            sorted_invoices = sorted(customer_invoices, 
                                                   key=lambda x: x.get('date', ''), 
                                                   reverse=True)
                            related_invoice = sorted_invoices[0]
                            invoice_match_method = "Most Recent"
                    
                    record = {
                        'Job_ID': job_id,
                        'Job_Number': job_number,
                        'Job_Name': job_name,
                        'Customer_ID': customer_id,
                        'Customer_Name': customer_name,
                        'Job_Created_Date': job.get('created_at'),
                        'ECD_Estimated_Completion_Date': job.get('end_date'),
                        'Invoice_Date': related_invoice.get('date') if related_invoice else None,
                        'Invoice_ECD_Due_Date': self.calculate_due_date(related_invoice.get('date') if related_invoice else None),
                        'Job_Project_Value': job.get('total', 0),
                        'Invoice_Total': related_invoice.get('total') if related_invoice else None,
                        'Business_Category': job.get('category') or 'General',
                        'Job_Status': job.get('status'),
                        'Payment_Status': job.get('payment_status'),
                        'Has_Related_Invoices': related_invoice is not None,
                        'Invoice_Count': len(customer_invoices),
                        'Invoice_Match_Method': invoice_match_method
                    }
                    
                    summary_records.append(record)
                    
                except Exception as e:
                    logger.warning(f"Error processing job: {str(e)}")
                    continue
            
            logger.info(f"Created {len(summary_records)} job records")
            
            df = pd.DataFrame(summary_records)
            
            # Remove duplicates and clean dates
            if not df.empty:
                df = df.drop_duplicates(subset=['Job_ID'], keep='first')
                
                for col in ['Job_Created_Date', 'ECD_Estimated_Completion_Date', 'Invoice_Date', 'Invoice_ECD_Due_Date']:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error creating Power BI summary: {str(e)}")
            return pd.DataFrame()

    def extract_all_data(self, date_range: str = "Year to Date") -> Dict[str, List[Dict]]:
        """Extract all data from Service Fusion API"""
        endpoints_to_extract = ['customers', 'jobs', 'estimates', 'invoices']
        all_data = {}
        
        logger.info(f"Starting {date_range} extraction for all endpoints")
        
        for endpoint_name in endpoints_to_extract:
            try:
                start_time = time.time()
                raw_data = self.extract_with_forced_sort(endpoint_name, date_range)
                end_time = time.time()
                
                all_data[endpoint_name] = raw_data
                logger.info(f"{endpoint_name}: {len(raw_data)} records in {end_time - start_time:.1f}s")
                
            except Exception as e:
                logger.error(f"Failed to extract {endpoint_name}: {str(e)}")
                all_data[endpoint_name] = []
        
        return all_data

    def save_to_excel(self, all_data: Dict[str, List[Dict]], filename: str):
        """Save extracted data to Excel file"""
        try:
            logger.info(f"Saving data to {filename}")
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                
                # Create PowerBI summary
                powerbi_df = self.create_powerbi_summary(all_data)
                
                if not powerbi_df.empty:
                    powerbi_df.to_excel(writer, sheet_name='PowerBI_Summary', index=False)
                    logger.info(f"PowerBI_Summary: {len(powerbi_df)} records")
                else:
                    # Create empty sheet with headers
                    empty_powerbi = pd.DataFrame(columns=[
                        'Job_ID', 'Job_Number', 'Job_Name', 'Customer_ID', 'Customer_Name',
                        'Job_Created_Date', 'ECD_Estimated_Completion_Date', 'Invoice_Date', 
                        'Invoice_ECD_Due_Date', 'Job_Project_Value', 'Invoice_Total',
                        'Business_Category', 'Job_Status', 'Payment_Status', 
                        'Has_Related_Invoices', 'Invoice_Count', 'Invoice_Match_Method'
                    ])
                    empty_powerbi.to_excel(writer, sheet_name='PowerBI_Summary', index=False)
                    logger.info("Created empty PowerBI_Summary sheet")
                
                # Create extraction summary
                summary_data = []
                for endpoint, data in all_data.items():
                    summary_data.append({
                        'Endpoint': endpoint,
                        'Record Count': len(data),
                        'Extracted At': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Extraction_Summary', index=False)
                
                # Save raw data
                for endpoint_name, data in all_data.items():
                    if data:
                        df = pd.DataFrame(data)
                        df.to_excel(writer, sheet_name=endpoint_name[:31], index=False)
                        logger.info(f"Saved {endpoint_name}: {len(data)} records")
            
            logger.info(f"Successfully saved {filename}")
            
        except Exception as e:
            logger.error(f"Error saving Excel file: {str(e)}")
            raise


def main():
    """Main execution function"""
    try:
        # Debug: Print environment info
        print(f"Python version: {sys.version}")
        print(f"Current working directory: {os.getcwd()}")
        print("Environment variables:")
        print(f"SF_CLIENT_ID exists: {'SF_CLIENT_ID' in os.environ}")
        print(f"SF_CLIENT_SECRET exists: {'SF_CLIENT_SECRET' in os.environ}")
        
        # Get credentials from environment
        client_id = os.getenv('SF_CLIENT_ID')
        client_secret = os.getenv('SF_CLIENT_SECRET')
        date_range = os.getenv('DATE_RANGE', 'Year to Date')
        
        if not client_id or not client_secret:
            logger.error("Missing SF_CLIENT_ID or SF_CLIENT_SECRET environment variables")
            sys.exit(1)
        
        logger.info(f"Starting Service Fusion extraction ({date_range})")
        
        # Create extractor and run
        extractor = ServiceFusionExtractor(client_id, client_secret)
        
        # Extract data
        all_data = extractor.extract_all_data(date_range)
        
        # Create filename
        filename = f"ServiceFusion_YTD_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx"
        
        # Save to Excel
        extractor.save_to_excel(all_data, filename)
        
        # Summary
        total_records = sum(len(data) for data in all_data.values())
        logger.info(f"Extraction completed: {total_records} total records saved to {filename}")
        
        return True
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
