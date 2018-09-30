from zenpy import Zenpy
import datetime
import calendar
import pandas as pd
Custom_Field_Values = {
    "25192963" : "Escalate_to_Sustenance",
    "24216323" : "Severity_Business_Impact",
    "22165409" : "Qubole_Account_Name",
    "22214565" : "Qubole_Command_IDs",
    "25379646" : "Rerun_Command_Permission",
    "24646363" : "Issue_Summary",
    "24653963" : "KB_Candidate",
    "24703466" : "UI_Engine_Problem_Encountered",
    "25356346" : "Linked_JIRA_IDs",
    "24216333" : "Type",
    "24287543" : "Support_Plan",
    "22165259" : "Qubole_User_ID",
    "24720283" : "Total_Effort_on_Ticket",
    "24720303" : "Effort_since_last_update"
}
class ZendeskMetrics:
    def __init__(self, user, token, NoOfdays):
        self.creds = { 'email':user, 'token': token, 'subdomain':'qubole'}
        self.client = Zenpy(**self.creds)
        self.startTime = datetime.datetime.now() - datetime.timedelta(days=NoOfdays)
        self.result_generator = self.client.tickets.incremental(start_time=self.startTime)
        #print(result_generator.__dir__())
        #Ticket_Data = CollectMetric()



    def CreateSchema(self):
        Custom_Field_Names = []
        i = 1
        SampleInterval = datetime.datetime.now() - datetime.timedelta(days=1)
        sample_result = self.client.tickets.incremental(start_time=SampleInterval)
        for ticket in sample_result:
            Custom_Fields = ticket.custom_fields
            for dictItem in Custom_Fields:
                if str(dictItem['id']) not in ("24720303", "22165409", "22214565", "22165259"):
                    checkCustomField = str(dictItem['id']) in Custom_Field_Values
                    if checkCustomField:
                        Custom_Field_Names.append(Custom_Field_Values[str(dictItem['id'])])
                    else:
                        Custom_Field_Names.append("New Field " + str(i))
                        i = i + 1
            break
        self.Table_Schema = ['Ticket_Id','Ticket_Subject', 'Ticket_Status', 'Assignee', 'Requester', 'Organization', 'Priority'] + Custom_Field_Names + ['Created_At', 'Last_Updated_At']
        return self.Table_Schema

    #DateStr = '2018-09-12T06:26:57Z'
    def ConvertStrToDatetime(self, DateStr):
        StripDate = DateStr.replace('T', ' ').replace('Z', '')
        #print(StripDate)
        self.DateForm = datetime.datetime.strptime(StripDate, '%Y-%m-%d %H:%M:%S')
        #print(DateForm)
        return self.DateForm
        #print(DateForm)
        #print(type(DateForm))

    def CollectMetrics(self):
        self.All_Tickets = []
        for ticket in self.result_generator:
            ticket_details = []
            try:
                ticket_details.append(ticket.id)
            except:
                ticket_details.append(00000)
            try:
                ticket_details.append(ticket.subject)
            except:    
                ticket_details.append('Subject Missing')
            try:
                ticket_details.append(ticket.status)
            except:
                ticket_details.append('Status Missing')
            try:
                ticket_details.append(self.client.users(id=ticket.assignee_id).name)
            except:
                ticket_details.append('Unassigned')
            try:
                ticket_details.append(self.client.users(id=ticket.requester_id).name)
            except:
                ticket_details.append('Unknown')  
            try:
                ticket_details.append(self.client.organizations(id=ticket.organization_id).name)                      
            except:
                ticket_details.append('Unknown')
            try:
                ticket_details.append(ticket.priority)
            except:
                ticket_details.append('Unknown')
            List_custom_Fields = ticket.custom_fields
            for dictItem in List_custom_Fields:
                if str(dictItem['id']) not in ("24720303", "22165409", "22214565", "22165259"):
                    #if str(dictItem['id']) == "24703466":
                    #    UI_Engine_Split_List = str(dictItem['value']).split('__')
                    #    ticket_details.append(UI_Engine_Split_List[0])
                    #    ticket_details.append(UI_Engine_Split_List[1])
                    #    continue
                    if str(dictItem['id']) == "24720283":
                        try:
                            ticket_details.append(int(dictItem['value'])) 
                        except:
                            ticket_details.append(0)
                        continue
                    if str(dictItem['value']) == "None":
                        dictItem['value'] = ""
                    try:
                        ticket_details.append(str(dictItem['value'])) 
                    except:
                        ticket_details.append('')                   
            CreatedAtDate = self.ConvertStrToDatetime(ticket.created_at)
            try:
                ticket_details.append(CreatedAtDate)
            except:
                ticket_details.append(datetime.datetime(1971, 1, 1, 0, 0, 0))
            UpdatedAtDate = self.ConvertStrToDatetime(ticket.updated_at)                          
            try:
                ticket_details.append(UpdatedAtDate)  
            except:
                ticket_details.append(datetime.datetime(1971, 1, 1, 0, 0, 0))
            #print(ticket_details)
            self.All_Tickets.append(ticket_details)
        return self.All_Tickets
        #	print(All_Tickets[4])

    def CreatePandasDF(self, All_Tickets, Schema):
        df =pd.DataFrame(self.All_Tickets, columns = Schema)
        #df[['Total_Effort_on_Ticket']] = df[['Total_Effort_on_Ticket']].astype(float).astype(int)   
        return df
        #print(df.dtypes)






if __name__ == "__main__":
    user = 'tusharn@qubole.com'
    token = '<token>'
    number_of_days = 7
    ZendeskMetricsObj = ZendeskMetrics(user, token, number_of_days)
    Schema = ZendeskMetricsObj.CreateSchema()
    All_Tickets_Data = ZendeskMetricsObj.CollectMetrics()
    df = ZendeskMetricsObj.CreatePandasDF(All_Tickets_Data, Schema)
    #print(df['Ticket_Status'].unique())
    #print(df['Assignee'].unique())
    FilterDF = df[(df.Assignee != "Unassigned") & (df.Ticket_Status != "deleted") & (df.Ticket_Id != 00000)]
    FilterDF.to_csv("Data.csv", index=False)
    #print(FilterDF['Assignee'].unique())
    #print(FilterDF['Ticket_Status'].unique())
        
    #print(df.iloc[3])
    #for i in df.iloc[3]:
    #    print(type(i))
