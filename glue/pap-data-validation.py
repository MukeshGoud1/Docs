from io import StringIO
from pathlib import Path
from awsglue.utils import getResolvedOptions
import subprocess
import traceback
import sys
try:
    from pap_validation.New_Hires_Validation import validate_new_hires
    from pap_validation.EDR_Validation import validate_EDR
    from pap_validation.Terminations_Validation import validate_terminations
    from pap_validation.Movements_Validation import validate_movements
    from pap_validation.Applicants_Validation import validate_applicants
    from datetime import datetime, timedelta
    from tabulate import tabulate
    from pap_validation.validation_utils import send_html_email, error_email, get_config
    import boto3
except Exception as e:
    print(e)
    raise e
# from pap_validation.setup import summary_recipients, email_folder, error_recipients

print("job started")

args = getResolvedOptions(sys.argv, ['config_bucket', 'app_config_object_key'])
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
list1=[]
#bucket name

CODE_ARTIFACTS_BUCKET=args['config_bucket']  #f"gilead-gna-hr-tst-us-west-2-code-artifacts"
print(CODE_ARTIFACTS_BUCKET)



# Set up and send the summary email w/ an HTML table
def send_summary_email(path):
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    subject = f"PAP Reconciliation Report for Data as of {yesterday}"
    message_html = '''
    <html>
        <head>
            <style type="text/css">
                body {{ font-family: 'Calibri'; font-size: 11pt; }}
                table {{ border: 1px black solid; border-collapse: collapse; }}
                th {{ background-color: #4f81bd; color: #ffffff; border: 1px black solid;
                      padding-left: 4.8px; padding-right: 4.8px; }}
                td {{ border: 1px black solid; padding-left: 4.8px; padding-right: 4.8px; }}
            </style>
        </head>
        <body>
        <p> Hi all,</p>

        <p>Below is the consolidated data reconciliation summary between Workday and PAP as of {yesterday}. Detailed information can be found in:<br><a href="{path}">{path}</a><br><br>

    '''
    if edr_flag:
        if 'EDR_validation_data' not in globals():
            global EDR_missing_records, EDR_dup_records, EDR_total_errors, EDR_table
            EDR_missing_records = None,
            EDR_dup_records = None,
            EDR_total_errors = None
            EDR_table = None
            message_html += \
                '''
            <b>EDR Recon Summary:</b><br>
            <p>EDR validation failed. An email containing the error details has been sent separately.</p>
            '''
        else:
            message_html += \
                '''
            <b>EDR Recon Summary:</b><br>
            <span class="missing">{EDR_missing_records}</span><br>
            <span class="duplicate">{EDR_dup_records}</span><br>
            <span class="total_errors">Number of records with errors: {EDR_total_errors}</span><br><br>
            {EDR_table}
            '''
            EDR_table = tabulate(EDR_validation_data, headers=EDR_validation_headers, tablefmt='html')

    if new_hires_flag:
        if 'new_hires_validation_data' not in globals():
            global new_hires_missing_records, new_hires_dup_records, new_hires_total_errors, new_hires_table
            new_hires_missing_records = None,
            new_hires_dup_records = None,
            new_hires_total_errors = None
            new_hires_table = None
            message_html += \
                '''
            <br><br>
            <b>New Hires Recon Summary:</b><br>
            <p>New Hires validation failed. An email containing the error details has been sent separately.</p>
            '''
        else:
            message_html += \
                '''
            <br><br>
            <b>New Hires Recon Summary:</b><br>
            <span class="missing">{new_hires_missing_records}</span><br>
            <span class="duplicate">{new_hires_dup_records}</span><br>
            <span class="total_errors">Number of records with errors: {new_hires_total_errors}</span><br><br>
            {new_hires_table}
            '''
            new_hires_table = tabulate(new_hires_validation_data, headers=new_hires_validation_headers, tablefmt='html')

    if terminations_flag:
        if 'terminations_validation_data' not in globals():
            global terminations_missing_records, terminations_dup_records, terminations_total_errors, terminations_table
            terminations_missing_records = None
            terminations_dup_records = None
            terminations_total_errors = None
            terminations_table = None
            message_html += \
                '''
            <br><br>
            <b>Terminations Recon Summary:</b><br>
            <p>Terminations validation failed. An email containing the error details has been sent separately.</p>
            '''
        else:
            message_html += \
                '''
            <br><br>
            <b>Terminations Recon Summary:</b><br>
            <span class="missing">{terminations_missing_records}</span><br>
            <span class="duplicate">{terminations_dup_records}</span><br>
            <span class="total_errors">Number of records with errors: {terminations_total_errors}</span><br><br>
            {terminations_table}
            '''
            terminations_table = tabulate(terminations_validation_data, headers=terminations_validation_headers,
                                          tablefmt='html')

    if movements_flag:
        if 'movements_validation_data' not in globals():
            global movements_missing_records, movements_dup_records, movements_total_errors, movements_table
            movements_missing_records = None
            movements_dup_records = None
            movements_total_errors = None
            movements_table = None
            message_html += \
                '''
            <br><br>
            <b>Movements Recon Summary:</b><br>
            <p>Movements validation failed. An email containing the error details has been sent separately.</p>
            '''
        else:
            message_html += \
                '''
            <br><br>
            <b>Movements Recon Summary:</b><br>
            <span class="missing">{movements_missing_records}</span><br>
            <span class="duplicate">{movements_dup_records}</span><br>
            <span class="total_errors">Number of records with errors: {movements_total_errors}</span><br><br>
            {movements_table}
            '''
            movements_table = tabulate(movements_validation_data, headers=movements_validation_headers, tablefmt='html')

    if applicants_flag:
        if 'applicants_validation_data' not in globals():
            global applicants_missing_records, applicants_dup_records, applicants_total_errors, applicants_table
            applicants_missing_records = None
            applicants_dup_records = None
            applicants_total_errors = None
            applicants_table = None
            message_html += \
                '''
            <br><br>
            <b>Applicants Recon Summary:</b><br>
            <p>Applicants validation failed. An email containing the error details has been sent separately.</p>
            '''
        else:
            message_html += \
                '''
            <br><br>
            <b>Applicants Recon Summary:</b><br>
            <span class="missing">{applicants_missing_records}</span><br>
            <span class="duplicate">{applicants_dup_records}</span><br>
            <span class="total_errors">Number of records with errors: {applicants_total_errors}</span><br><br>
            {applicants_table}
            '''

            applicants_table = tabulate(applicants_validation_data, headers=applicants_validation_headers,
                                        tablefmt='html')

    message_html += \
        '''
        </body>
    </html>
        '''

    message_html = message_html.format(yesterday=yesterday, \
                                       path=path, new_hires_total_errors=new_hires_total_errors, \
                                       EDR_missing_records=EDR_missing_records, EDR_dup_records=EDR_dup_records,
                                       EDR_total_errors=EDR_total_errors, EDR_table=EDR_table, \
                                       new_hires_missing_records=new_hires_missing_records,
                                       new_hires_dup_records=new_hires_dup_records, new_hires_table=new_hires_table, \
                                       terminations_missing_records=terminations_missing_records,
                                       terminations_dup_records=terminations_dup_records,
                                       terminations_total_errors=terminations_total_errors,
                                       terminations_table=terminations_table, \
                                       movements_missing_records=movements_missing_records,
                                       movements_dup_records=movements_dup_records,
                                       movements_total_errors=movements_total_errors, movements_table=movements_table, \
                                       applicants_missing_records=applicants_missing_records,
                                       applicants_dup_records=applicants_dup_records,
                                       applicants_total_errors=applicants_total_errors,
                                       applicants_table=applicants_table)
    send_html_email(subject, message_html, summary_recipients)
    return


# If a script failed, the headers list contains a list of these errors.
# Append each of these to the error email if applicable
def check_headers(headers, email):
    email.are_errors = True
    for error in headers:
        email.body += error
    email.body += "<br><br>"
    return email


if __name__ == "__main__":
    try:
        # Define global variables to be used when building email body:
        global applicants_Validation_headers, applicants_validation_data, applicants_total_errors, applicants_missing_records, applicants_dup_records, \
            applicants_table, movements_validation_headers, movements_validation_data, movements_total_errors, \
            movements_missing_records, movements_dup_records, terminations_validation_headers, movements_table, \
            terminations_validation_data, terminations_total_errors, terminations_missing_records, \
            terminations_dup_records, terminations_table, new_hires_validation_headers, new_hires_validation_data, \
            new_hires_total_errors, new_hires_missing_records, new_hires_dup_records, new_hires_table, \
            EDR_validation_headers, EDR_missing_records, EDR_dup_records, EDR_table, EDR_total_errors, missing_files
    
        global edr_flag, new_hires_flag, terminations_flag, movements_flag, applicants_flag
        
        edr_flag = True
        new_hires_flag = True
        terminations_flag = True
        movements_flag = True
        applicants_flag = True
        args = getResolvedOptions(sys.argv, ['config_bucket', 'app_config_object_key'])
        app_config_yml = get_config(args['config_bucket'], args['app_config_object_key'])['python_recon_process']
        print("app_config_yml", type(app_config_yml))
        summary_recipients, email_folder = app_config_yml['summary_recipients'], app_config_yml['email_folder']
        error_recipients = app_config_yml['error_recipients']
        print(summary_recipients, email_folder, error_recipients)
        print("execution started")
        # If specific scripts are specified in command line arguments, only activate these.
        # if len(sys.argv) > 1:
        #     edr_flag = False
        #     EDR_missing_records = EDR_dup_records = EDR_total_errors = EDR_table = None
    
        #     new_hires_flag = False
        #     new_hires_missing_records = new_hires_dup_records = new_hires_total_errors = new_hires_table = None
    
        #     terminations_flag = False
        #     terminations_missing_records = terminations_dup_records = terminations_total_errors = terminations_table = None
    
        #     movements_flag = False
        #     movements_missing_records = movements_dup_records = movements_total_errors = movements_table = None
    
        #     applicants_flag = False
        #     applicants_missing_records = applicants_dup_records = applicants_total_errors = applicants_table = None
        #     print("after variable setting")
    
        #     for a in sys.argv[1:]:
        #         if a.lower() == "edr":
        #             edr_flag = True
        #         elif a.lower() == "hires":
        #             new_hires_flag = True
        #         elif a.lower() == "terminations":
        #             terminations_flag = True
        #         elif a.lower() == "movements":
        #             movements_flag = True
        #         elif a.lower() == "applicants":
        #             applicants_flag = True
        #         else:
        #             print(f"Unknown script name: {a}")
        #             exit()
    
        execution_errors = {}
    
        e = error_email()
        e.subject = "Error - PAP/WD Data Validation Missing Files"
    
        if edr_flag:
            print(f"Starting EDR validation...")
            try:
                print("started edr report")
                EDR_validation_headers, EDR_validation_data, EDR_total_errors, EDR_missing_records, EDR_dup_records, error_flag = validate_EDR()
                print(" edr error",error_flag)
                if error_flag: e = check_headers(EDR_validation_headers, e)
            except Exception as ex:
                execution_errors["EDR Validation"] = traceback.format_exc()
                print(f"\nValidation halted due to error: {ex}")
    
        if new_hires_flag:
            print(f"\n\nStarting new hires validation...")
            try:
                new_hires_validation_headers, new_hires_validation_data, new_hires_total_errors, new_hires_missing_records, new_hires_dup_records, error_flag = validate_new_hires()
                if error_flag: e = check_headers(new_hires_validation_headers, e)
            except Exception as ex:
                execution_errors["New Hires Validation"] = traceback.format_exc()
                print(f"\nValidation halted due to error: {ex}")
    
        if terminations_flag:
            print(f"\n\nStarting terminations validation...")
            try:
                terminations_validation_headers, terminations_validation_data, terminations_total_errors, terminations_missing_records, terminations_dup_records, error_flag = validate_terminations()
                if error_flag: e = check_headers(terminations_validation_headers, e)
            except Exception as ex:
                execution_errors["Terminations Validation"] = traceback.format_exc()
                print(f"\nValidation halted due to error: {ex}")
    
        if movements_flag:
            print(f"\n\nStarting movements validation...")
            try:
                movements_validation_headers, movements_validation_data, movements_total_errors, movements_missing_records, movements_dup_records, error_flag = validate_movements()
                if error_flag: e = check_headers(movements_validation_headers, e)
            except Exception as ex:
                execution_errors["Movements Validation"] = traceback.format_exc()
                print(f"\nValidation halted due to error: {ex}")
    
        if applicants_flag:
            print(f"\n\nStarting applicants validation...")
            try:
                applicants_validation_headers, applicants_validation_data, applicants_total_errors, applicants_missing_records, applicants_dup_records, error_flag = validate_applicants()
                if error_flag: e = check_headers(applicants_validation_headers, e)
            except Exception as ex:
                execution_errors["Applicants Validation"] = traceback.format_exc()
                print(f"\nValidation halted due to error: {ex}")
    
        if len(execution_errors.keys()):
            subject = "Error - PAP/WD Data Validation Failed"
            body = \
                '''
            <html>
                <body style="font-family: 'Calibri'; font-size: 11pt;">
                <p>The following exceptions were raised during data validation:</p><br><br>
            '''
    
            for title, msg in execution_errors.items():
                body += \
                    f'''
                <b>{title}:</b><br>
                <pre style="font-family: monospace; font-size: 10pt; background: #f4f4f4; color: #777; border: 2px solid #ccc; border-radius: 5px; break-inside: avoid; word-wrap: break-word; padding: 10px; word-wrap: break-word;">{msg}</pre><br><br>
                '''
    
            body += \
                '''
                </body>
            </html>
            '''
    
            send_html_email(subject, body, error_recipients)
    
        if e.are_errors:
            print("inside error box")
            yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            e.body += '''
                    <p>Data validation cannot be completed until all files for {yesterday} are obtained,
                    so please attend to this at your earliest convenience.<br><br>
    
                    Thank you,<br>
                    People Analytic Team</p>
                </body>
            </html>
            '''
            e.body = e.body.format(yesterday=yesterday)
            send_html_email(e.subject, e.body, error_recipients)
        send_summary_email(email_folder)

    except Exception as ex:
        print(f"\nValidation halted due to error: {ex}")