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

    def create_Group(self, bankid, CIF, GroupId, CreatedBy, Status):
        existing_group = self.df[
            (self.df['bankid'] == bankid) &
            (self.df['CIF'] == CIF) &
            (self.df['GroupId'] == GroupId) &
            (self.df['Status'] == Status)
        ]

        if not existing_group.empty:
            return "Group already exists"

            r1Group = MyGroupClass(bankid, CIF, GroupId, CreatedBy, Status)

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

    def update_Group(self, group_id, bankid=None, CIF=None, GroupId=None, Status=None, appstatus=None, ModifiedBy=None):
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
            "obj": "GroupMaster",
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
            p_rule = self.df[(self.df['id'] == accblock_id) & (self.df['appApprovedby'].notnull())]
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
                    "obj": "GroupMaster",
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






