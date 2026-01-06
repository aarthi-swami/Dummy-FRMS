import pandas as pd
import time
import traceback
from datetime import datetime
import time
from flask import Flask, session, request, render_template, redirect, url_for
from sqlalchemy import text
from app.LogsImport import log_error_to_database, log_event
from app.FRMDBOperations import get_SQL_engine

app = Flask(__name__)


class GroupMembersclass:
    def __init__(self, bankid, CIF, GroupId, CreatedBy, Status):
        self.id = str(int(time.time()))
        self.bankid = bankid
        self.CIF = CIF
        self.GroupId = GroupId
        self.appAction = 'Pending'
        self.Status = Status
        self.created_by = CreatedBy
        self.created_on = datetime.now()
        self.modified_by = ''
        self.modified_on = ''
        self.appApprovedby = ''
        self.reservedfield1 = ''


class GroupManagementManager:
    def __init__(self, user, df, del_df, engine, conn):
        self.user = user
        self.df = df
        self.deleted_df = del_df
        self.engine = engine
        self.conn = conn


    def format_sql_value(self, val):
        if pd.isna(val):
            return 'NULL'
        elif isinstance(val, pd.Timestamp):  # Handle Pandas Timestamp
            return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:  # Handle all other cases
            return val


    def save_to_database(self, savedf, dbaction, groupid=None):
        with get_SQL_engine().connect() as connection:
            try:
                if dbaction == 'insert':
                    savedf = savedf.drop_duplicates()
                    # Insert into GroupMaster table
                    savedf.to_sql(name='GroupMaster', con=connection, if_exists='append', index=False)

                elif dbaction == 'update':
                    for _, row in savedf.iterrows():
                        row['ReservedField1'] = ''
                        case_id = row['Id']  # ID to identify the row
                        set_clause = ", ".join(
                            [f"{col} = {self.format_sql_value(val)}" for col, val in row.items()]
                        )
                        sql_query = f"UPDATE GroupMaster SET {set_clause} WHERE Id = {repr(case_id)}"
                        print(f"Executing SQL: {sql_query}")
                        connection.execute(text(sql_query))

                elif dbaction == 'delete':
                    sql_query = f"DELETE FROM GroupMaster WHERE Id = {repr(groupid)}"
                    print(f"Executing SQL: {sql_query}")
                    connection.execute(text(sql_query))

                connection.commit()

            except Exception as e:
                print(f"An error occurred: {e}")
                connection.rollback()
            finally:
                connection.close()

    def create_Group(self, bankid, AccountNo, GroupId, CreatedBy, Status):
        existing_group = self.df[
            (self.df['bankid'] == bankid) &
            (self.df['CIF'] == AccountNo) &
            (self.df['GroupId'] == GroupId) &
            (self.df['Status'] == Status)
        ]

        if not existing_group.empty:
            return "Group already exists"

            r1Group = MyGroupClass(bankid, AccountNo, GroupId, CreatedBy, Status)

            Group_data = {
                'id': r1Group.id,
                'bankid': bankid,
                'CIF': AccountNo,
                'GroupId': GroupId,
                'appAction': r1Group.appAction,
                'created_on': r1Group.CreatedOn,
                'created_by': r1Group.CreatedBy,
                'reservedfield1': '',
                'Status': Status,
                'appApprovedby': None
            }

            userdetails = session.get('userdetails')
            username = userdetails.get('UserName')
            id = f'log_{int(time.time())}'
            navlink = '/Group_Management_module/GroupManagement'
            obj_id = f'group_{r1Group.id}'
            bank_session_id = userdetails.get('bankid')
            ActionType = "Maker"

            self.df = pd.concat([self.df, pd.DataFrame([Group_data])], ignore_index=True)
            self.save_to_database(pd.DataFrame([Group_data]), 'insert')

            log_event(log_id, f"{username}",f'Group created', navlink,
              obj_id, bank_session_id, ActionType, self.conn
              )
            return r1Group.id

    def update_Group(self, group_id, bankid=None, AccountNo=None, GroupId=None, Status=None, appstatus=None, ModifiedBy=None):
        userdetails = session.get('userdetails')
        username = userdetails.get('UserName')
        id = f'log_{int(time.time())}'
        navlink = '/Group_Management_module/GroupManagement'
        obj_id = f'group_{group_id}'
        bankid = session.get('userdetails')['bankid']
        ActionType = "Maker"

        if appstatus is not None and appstatus == 'Approved' and self.df.loc[
            self.df['id'] == group_id, 'reservedfield1'].isnull().all():
            self.df.loc[self.df['id'] == str(group_id), 'appAction'] = 'Approved'
            self.df.loc[self.df['id'] == str(group_id), 'modified_on'] = datetime.now()
            self.save_to_database(self.df[self.df['id'] == str(group_id)], 'update', group_id)

            payload = {
                "obj": "tbl_GroupMembers",
                "rowvals": [
                    int(group_id),
                    self.df.loc[self.df['id'] == group_id, 'bankid'].values[0],
                    self.df.loc[self.df['id'] == group_id, 'CIF'].values[0],
                    self.df.loc[self.df['id'] == group_id, 'GroupId'].values[0],
                    self.df.loc[self.df['id'] == group_id, 'Status'].values[0]
                ],
                "Ckeycols": ["id", "bankid", "CIF", "GroupId", "Status"],
                "wherevals": [group_id],
                "wherecols": ["id"],
                "operation": "insert"
            }

            # url = config["IgniteDumpingUrl"]
            # response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
            log_event(id, f"{username}", f'Approved group update', navlink, obj_id, bankid, ActionType, self.conn)

            return group_id

        elif appstatus is 'Approved' and self.df['reservedfield1'] is not None:
            self.df.loc[self.df['id'] == group_id, 'appAction'] = appstatus
            p_rule = self.df[(self.df['id'] == group_id) & (self.df['appApprovedby'].notnull())]
            appApprovedby_value = str(p_rule.iloc[0]['appApprovedby'])

        if appApprovedby_value:
            p_rule = self.df[self.df['id'] == group_id]
            p_rule = self.df[self.df['id'] == group_id].drop(columns=['id'])
            self.df.loc[self.df['id'] == int(appApprovedby_value), p_rule.columns] = p_rule.values
            self.df = self.df[~self.df['id'].isin([int(group_id)])]
            self.df.loc[self.df['id'] == group_id, 'appApprovedby'] = ''
            self.df.loc[self.df['id'] == group_id, 'id'] = int(appApprovedby_value)
            old_id = group_id
            group_id = int(appApprovedby_value)
            self.save_to_database(self.df.loc[self.df['id'] == group_id], 'update', group_id)
            self.save_to_database(self.df.loc[self.df['id'] == old_id], 'delete', old_id)
            payload = {
                "obj": "tbl_GroupMembers",
                "rowvals": [
                    self.df.loc[self.df['id'] == group_id, 'bankid'].values[0],
                    self.df.loc[self.df['id'] == group_id, 'CIF'].values[0],
                    self.df.loc[self.df['id'] == group_id, 'GroupId'].values[0],
                    self.df.loc[self.df['id'] == group_id, 'Status'].values[0]
                ],
                "Ckeycols": ["bankid", "CIF", "GroupId", "Status"],
                "wherevals": [group_id],
                "wherecols": ["id"],
                "operation": "update"
            }

            # url = config["IgniteDumpingUrl"]
            # response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
            log_event(id, f"{username}", f'Approved group update', navlink, obj_id, bankid, ActionType, self.conn)

            return group_id

        elif appstatus is 'Approved' and self.df['reservedfield1'] is not None:\
        self.df.loc[self.df['id'] == group_id, 'appAction'] = appstatus
        p_rule = self.df[(self.df['id'] == group_id) & (self.df['appApprovedby'].notnull())]
        appApprovedby_value = str(p_rule.iloc[0]['appApprovedby'])

        if appApprovedby_value:
            p_rule = self.df[self.df['id'] == group_id]
            p_rule = self.df[self.df['id'] == group_id].drop(columns=['id'])
            self.df.loc[self.df['id'] == int(appApprovedby_value), p_rule.columns] = p_rule.values
            self.df = self.df[~self.df['id'].isin([int(group_id)])]
            self.df.loc[self.df['id'] == group_id, 'appApprovedby'] = ''
            self.df.loc[self.df['id'] == group_id, 'id'] = int(appApprovedby_value)
            old_id = group_id
            group_id = int(appApprovedby_value)
            self.save_to_database(self.df.loc[self.df['id'] == group_id], 'update', group_id)
            self.save_to_database(self.df.loc[self.df['id'] == old_id], 'delete', old_id)
            payload = {
                "obj": "tbl_GroupMembers",
                "rowvals": [
                    self.df.loc[self.df['id'] == group_id, 'bankid'].values[0],
                    self.df.loc[self.df['id'] == group_id, 'CIF'].values[0],
                    self.df.loc[self.df['id'] == group_id, 'GroupId'].values[0],
                    self.df.loc[self.df['id'] == group_id, 'Status'].values[0]
                ],
                "Ckeycols": ["bankid", "CIF", "GroupId", "Status"],
                "wherevals": [group_id],
                "wherecols": ["id"],
                "operation": "update"
            }
            url = config["IgniteDumpingUrl"]
            # response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
            # print(response.status_code)
            ActionType = "Approver"
            log_event(id, f"{username}", f'Approved group update',
                      navlink,
                      obj_id, bankid, ActionType, self.conn)

            return group_id

        elif appstatus is not None and appstatus is 'Declined':
            self.df.loc[self.df['id'] == str(group_id), 'appAction'] = 'Declined'
            self.df.loc[self.df['id'] == str(group_id), 'ModifiedOn'] = datetime.now()
            log_event(id, f"{username}", f'Declined group update', navlink,
                      obj_id, bankid, ActionType, self.conn)
            self.save_to_database(self.df.loc[self.df['id'] == str(group_id)], 'update', str(group_id))

        else:
            # self.df.loc[self.df['id'] == group_id, 'appAction'] = 'Pending'
            if group_id in self.df['id'].values:
                old_row = self.df[self.df['id'] == group_id].iloc[0]
                copyRow = old_row.copy()
                copyRow['id'] = int(time.time())  # Set new RoleID as the current length of the DataFrame + 1
                copyRow['appAprrovedby'] = int(group_id)
                copyRow['appAction'] = 'Pending'
                # old_row = self.df[self.df['id'] == group_id].iloc[0]
                self.df.loc[len(self.df)] = copyRow
                group_id = copyRow['id']
                # self.save_to_database(self.df)
                old_values = {
                    'bankid': old_row['bankid'],
                    'CIF': old_row['CIF'],
                    'GroupId': old_row['GroupId']
                }
                if bankid:
                    self.df.loc[self.df['id'] == group_id, 'bankid'] = bankid
                if AccountNo:
                    self.df.loc[self.df['id'] == group_id, 'CIF'] = AccountNo
                if GroupId:
                    self.df.loc[self.df['id'] == group_id, 'GroupId'] = GroupId

                if Status:
                    self.df.loc[self.df['id'] == group_id, 'Status'] = Status

                self.df.loc[self.df['id'] == group_id, 'reservedfield1'] = str(old_values)
                userdetails = session.get('userdetails')
                new_row = self.df[self.df['id'] == group_id].iloc[0]
                new_values = {
                    'bankid': new_row['bankid'],
                    'CIF': new_row['CIF'],
                    'GroupId': new_row['GroupId'],
                    'Status': new_row['Status'],
                }

            self.save_to_database(self.df.loc[self.df['id'] == group_id], 'insert')
            log_event(f'log_{int(time.time())}', userdetails.get('UserName'), f'Group updated {old_values} to {new_values}', f'/Group_Management_module/GroupManagement', f'user_{group_id}', session['bankid'], "Maker", self.conn)

            return group_id

    def delete_group(self, group_id, appstatus=None, case=None):
        group_id = str(group_id)

        if appstatus == 'Approved':
            copy_row = self.df[self.df['id'] == group_id]
            original_id = copy_row['appAprrovedby'].values[0]
            self.df = self.df[~self.df['id'].isin([group_id, original_id])]
            userdetails = session.get('userdetails')
            self.save_to_database(self.df[self.df['id'].isin([group_id, original_id])], 'delete',
                                  [group_id, original_id])
            log_event(f'log_{int(time.time())}', userdetails.get('UserName'), 'Group deleted',
                      '/Group_Management_module/GroupManagement', f'group_{original_id}', session['bankid'],
                      "Maker", self.conn)

        else:
            old_row = self.df[self.df['id'] == group_id].iloc[0]
            copyRow = old_row
            copyRow['id'] = int(time.time())
            copyRow['appAprrovedby'] = group_id
            copyRow['appAction'] = 'Pending'
            self.df.loc[len(self.df)] = copyRow
            new_id = copyRow['id']

        if group_id in self.df['id'].values:
            self.df.loc[self.df['id'] == new_id, 'appAprrovedby'] = group_id
            self.df.loc[self.df['id'] == new_id, 'reservedfield1'] = 'delete'
            group = self.df[self.df['id'] == group_id]
            userdetails = session.get('userdetails')
            self.save_to_database(self.df[self.df['id'] == new_id], 'insert', new_id)
            log_event(f'log_{int(time.time())}', userdetails.get('UserName'), 'Group delete requested',
                      '/Group_Management_module/GroupManagement', f'group_{group_id}', session['bankid'],
                      "Maker", self.conn)

    def toggle_Group_status(self, GroupId, appstatus=None):

        if appstatus == 'Approved':
                copy_row = self.df[self.df['id'] == GroupId]
        if not copy_row.empty:
            original_id = str(copy_row['appAprrovedby'].values[0])
            current_status = self.df.loc[self.df['id'] == original_id, 'Status'].iloc[0]
            new_status = 'Active' if current_status == 'Inactive' else 'Inactive'
            self.df.loc[self.df['id'] == original_id, 'Status'] = new_status
            self.save_to_database(
                self.df.loc[self.df['id'] == original_id],'update', original_id)

            self.save_to_database(copy_row, 'delete', GroupId)

            payload = {
                "obj": "tbl_GroupMembers",
                "rowvals": [new_status],
                "Ckeycols": ["Status"],
                "wherevals": [original_id],
                "wherecols": ["id"],
                "operation": "toggle"
            }

            url = config["IgniteDumpingUrl"]
            # requests.post(url, data=payload)
            self.df = self.df[~self.df['id'].isin([int(GroupId)])]

        else:
            old_row = self.df[self.df['id'] == GroupId].iloc[0]
            copyRow = old_row.copy()

            copyRow['id'] = int(time.time())
            copyRow['appAprrovedby'] = GroupId
            copyRow['appAction'] = 'Pending'
            copyRow['reservedfield1'] = 'toggle'

            self.df.loc[len(self.df)] = copyRow
            group_id = copyRow['id']

            if GroupId in self.df['id'].values:
                current_status = self.df.loc[self.df['id'] == GroupId, 'Status'].iloc[0]
                new_status = 'Active' if current_status == 'Inactive' else 'Inactive'
                self.df.loc[self.df['id'] == group_id, 'Status'] = new_status
                userdetails = session.get('userdetails')
                username = userdetails.get('UserName')
                bankid = userdetails.get('bankid')
                ActionType = "Maker"
                log_id = f'log_{int(time.time())}'
                navlink = '/Group_Management_module/GroupManagement'
                obj_id = f'GroupId_{GroupId}'
                self.save_to_database(self.df[self.df['id'] == group_id],'insert',group_id)

                log_event(id, f"{username}", f'{username} changed status from {current_status} to {new_status}', navlink, obj_id, int(bankid),ActionType, self.conn)

        return new_status

    def get_GroupManagement(self):
        userdetails = session.get('userdetails')
        bankid = userdetails['bankid']
        query = "SELECT * FROM tbl_GroupMembers WHERE bankid = ?"
        with get_SQL_engine().connect() as connection:
            self.df = pd.read_sql(query, params=[(bankid,)], con=connection)

        return self.df