from airflow.models import Variable
from airflow.utils.email import send_email_smtp

from datetime import datetime

## Email variables ##
user_email_list = Variable.get("users_email_list", deserialize_json=True, default_var=None)
dag_name = Variable.get("dag_name", default_var=None)


layer = "BRONZE"
table = "breweries_data"
deleted = Variable.get("deleted", default_var=None)
updated = Variable.get("updated", default_var=None)
inserted = Variable.get("inserted", default_var=None)

def email_sender_crud(user_email_list, dag_name, deleted, updated, inserted):
    for email in user_email_list:
        print(f"Sending email to {email}...")
        try:  
            send_email_smtp(
                to = email,
                subject = f"Airflow {layer} layer: DAG {dag_name} has updated table {table}",
                html_content = f"""
                    <p class="MsoNormal"><b><span style='font-family:Arial",sans-serif;
                        color:green'>Notification:</span></b><span style='font-family:"Arial",sans-serif;
                        color:green'> Table {table} has been updated.</span><span style='font-family:Arial",sans-serif;
                        color:#1F497D'> <o:p></o:p></span></p>
                    <p class="MsoNormal"><b><span style='font-family:Arial",sans-serif;
                        color:white'> <o:p>&nbsp;</o:p></span></p>
                    <table class="MsoNormalTable" border="0" cellspacing="0" cellpadding="0" width="100%"
                        style='width:100.0%;border-collapse;mso-yfti-tbllook:1184;
                        mso-padding-alt:0in 0in 0in 0in'>
                        <tr style=mso-yfti-irow:0;mso_yfti-firstrow:yes;mso-yfti-lastrow:yes'>
                            <td width="50%" valign="top" style='width:50.0%';background:#444593;
                            padding:13.3pt 0in 13.3pt 27.35pt'>
                                <p class="MsoNormal"><b><span style='font-family:Arial",sans-serif;
                                color:white'>Deleted:</span></b><span style='font-family:"Arial",sans-serif;
                                color:black'> {deleted} records marked as deleted </span> <span style='font-family:"Arial",sans-serif;color:white'><o:p></o:p></span></p>
                                <p class="MsoNormal"><b><span style='font-family:Arial",sans-serif;
                                color:white'>Updated:</span></b><span style='font-family:"Arial",sans-serif;
                                color:black'> {updated} records have been updated <o:p></o:p></span></p>
                                <p class="MsoNormal"><b><span style='font-family:Arial",sans-serif;
                                color:white'>Inserted:</span></b><span style='font-family:"Arial",sans-serif;
                                color:black'> {inserted} records have been inserted </span> <span style='font-family:"Arial",sans-serif;color:white'><o:p></o:p></span></p>
                            </td>
                        </tr>
                    </table>
                """,
            )
            print('Email has been sent successfully!')
        except Exception as exception:
            print(exception)
            print("Failure")

if __name__ == "__main__":
    email_sender_crud(user_email_list, dag_name, deleted, updated, inserted)