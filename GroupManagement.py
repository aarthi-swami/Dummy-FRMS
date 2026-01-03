import pandas as pd
import time
import traceback
from datetime import datetime
from flask import Flask, session, request, render_template, redirect, url_for
from sqlalchemy import text

from app.LogsImport import log_error_to_database, log_event
from app.FRMDBOperations import get_SQL_engine

app = Flask(__name__)


class GroupManagementclass:
    def __init__(self, GroupName, bankid, created_by, Status='Active', appAction='Pending', reservedfield1=None, appAprrovedby=None):
        self.id = str(int(time.time()))
        self.GroupName = GroupName
        self.bankid = bankid
        self.Status = Status
        self.appAction = appAction
        self.created_on = datetime.now()
        self.created_by = created_by
        self.modified_on = None
        self.modified_by = None
        self.reservedfield1 = reservedfield1
        self.appAprrovedby = appAprrovedby


class GroupManager:
    def __init__(self, user, df, members_df, engine, conn):
        self.user = user
        self.df = df
        self.members_df = members_df
        self.engine = engine
        self.conn = conn

        def _format_sql_value(self, val):
            if pd.isna(val):
                return 'NULL'
            elif isinstance(val, pd.Timestamp):  # Handle Pandas Timestamp
                return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
            else:  # Handle all other cases
                return repr(val)

    def _log_error(self, e):
        tb = traceback.extract_tb(e.__traceback__)
        log_error_to_database(
            user_id=session.get('user1'),
            machine_ip=request.remote_addr,
            description=str(e),
            upload_filename=tb[-1].filename,
            line_no=tb[-1].lineno,
            method_name=tb[-1].name,
            upload_by="System"
        )

        return render_template('error.html', userdetails=session.get('userdetails'), error=str(e))

    def save_to_database(self, df, action, userid=None):
            query = 'SELECT * FROM GroupMaster WITH (NOLOCK);'
            groupmasterdf = pd.read_sql(query, con=self.engine)
            # group_map = groupmasterdf.set_index("GroupName").to_dict()["RoleID"]
            # df["RoleID"] = df["GroupName"].map(group_map)
            savedf = df.drop(["GroupName", "isid"], axis=1, errors="ignore")
        # engine = create_engine(config["engine"])

            with get_SQL_engine().connect() as connection:
                try:
                    if action == 'insert':
                        df.to_sql(name='GroupMaster', con=connection, if_exists='append', index=False)

                    elif action == 'update':
                        for _, row in df.iterrows():
                            case_id = row['id']
                            set_clause = ", ".join(
                                f"{col} = {self._format_sql_value(val)}"
                                for col, val in row.items() if col != 'id'
                            )
                            sql = f"UPDATE GroupMaster SET {set_clause} WHERE id = {repr(case_id)}"
                            connection.execute(text(sql))
                    elif action == 'delete':
                            if isinstance(userid, list):

                                    sql = f"DELETE FROM GroupMaster WHERE id IN ({', '.join([repr(id) for id in userid])}) or appAprrovedBy IN ({', '.join([repr(id) for id in userid])})"
                            else:
                                    sql = f"DELETE FROM GroupMaster WHERE id = {repr(userid)}"
                                    connection.execute(text(sql))
                                    connection.commit()

                except Exception as e:
                    print(f"An error occurred: {e}")  # Handle any errors
                    connection.rollback()  # Rollback in case of error
                finally:
                    connection.close()  # Ensure the connection is closed


    def create_group(self, group_name, bankid, Status, created_by, appAction='Pending', reservedfield1=None):

        try:
            existing_group = self.df[
                (self.df['GroupName'] == group_name) &
                (self.df['bankid'] == bankid) &
                (self.df['Status'] == Status) &
                (self.df['appAction'] == 'Approved')
                ]
            if not existing_group.empty:
                return "Group already exists"
            grp1 = GroupManagementclass(group_name, bankid, Status, created_by, appAction, reservedfield1, None)

            group_data = {
                'id': grp1.id,
                'GroupName': group_name,
                'bankid': bankid,
                'Status': Status,
                'appAction': grp1.appAction,
                'created_on': grp1.created_on,
                'created_by': grp1.created_by,
                'modified_by': None,
                'modified_on': None,
                'reservedfield1': None,
                'appAprrovedby': None
            }

            userdetails = session.get('userdetails')
            self.df = pd.concat([self.df, pd.DataFrame([group_data])], ignore_index=True)
            self.save_to_database(pd.DataFrame([group_data]), 'insert')
            log_event(f'log_{int(time.time())}', userdetails.get('UserName'),
                  f'Group created', '/Group_Management_module/GroupManagement',
                  f'user_{grp1.id}', session['bankid'], "Maker", self.conn)
            return int(grp1.id)

        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_number = tb[-1].lineno  # Get the line number of the exception
            file_name = tb[-1].filename  # Get the file name
            method_name = tb[-1].name
            log_error_to_database(
                user_id=session['user1'],
                machine_ip=request.remote_addr,
                description=str(e),
                upload_filename=file_name,  # If applicable
                line_no=line_number,
                method_name=method_name,
                upload_by="System"
            )
            return render_template('error.html', userdetails=session.get('userdetails'), error=str(e))


    def update_group(self, group_id, GroupName=None, Status=None, appstatus=None, modified_by=None):
        try:
            userdetails = session.get('userdetails')
            group_id = str(group_id)
            if appstatus is not None and appstatus == 'Approved' and self.df.loc[
                self.df['id'] == group_id, 'reservedfield1'].isnull().all():
                self.df.loc[self.df['id'] == group_id, 'appAction'] = appstatus
                self.save_to_database(self.df[self.df['id'] == group_id], 'update', group_id)
                log_event(f'log_{int(time.time())}', userdetails.get('UserName'), f'Approved user update',
                          '/Group_Management_module/GroupManagement',
                          f'user_{group_id}', session['bankid'], "Checker", self.conn)
                return group_id
            elif appstatus is 'Approved' and self.df['reservedfield1'] is not None:

                self.df.loc[self.df['id'] == group_id, 'appAction'] = appstatus
                p_rule = self.df[(self.df['id'] == group_id) & (self.df['appAprrovedby'].notnull())]
                appApprovedby_value = int(p_rule.iloc[0]['appAprrovedby'])
                if appApprovedby_value:
                    p_rule = self.df[self.df['id'] == group_id]
                    p_rule = self.df[self.df['id'] == group_id].drop(columns=['id'])

                    # Update all columns except `Id` for rows where `Id` matches `appApprovedby_value`
                    self.df.loc[self.df['id'] == int(appApprovedby_value), p_rule.columns] = p_rule.values
                    self.df = self.df[~self.df['id'].isin([int(group_id)])]
                    self.df.loc[self.df['id'] == group_id, 'appAprrovedby'] = ''
                    self.df.loc[self.df['id'] == group_id, 'id'] = int(appApprovedby_value)
                    old_id = group_id
                    role_id = int(appApprovedby_value)
                    self.save_to_database(self.df.loc[self.df['id'] == group_id], 'update', group_id)
                    self.save_to_database(self.df.loc[self.df['id'] == group_id], 'delete', old_id)
                    log_event(f'log_{int(time.time())}', userdetails.get('UserName'), f'Approved user creation ',
                              '/Group_Management_module/GroupManagement', f'user_{group_id}', session['bankid'], "Checker", self.conn)
                return group_id
            elif appstatus is not None and appstatus is 'Declined':
                self.df.loc[self.df['id'] == group_id, 'appAction'] = appstatus
                self.save_to_database(self.df.loc[self.df['id'] == group_id], 'update', group_id)
                log_event(f'log_{int(time.time())}', userdetails.get('UserName'), f'Declined user update ',
                          '/Group_Management_module/GroupManagement',
                          f'user_{group_id}', session['bankid'], "Checker", self.conn)
                return group_id
            else:
                # self.df.loc[self.df['id'] == group_id, 'appAction'] = 'Pending'
                if group_id in self.df['id'].values:
                    old_row = self.df[self.df['id'] == group_id].iloc[0]
                    copyRow = old_row.copy()
                    copyRow['id'] = int(time.time())  # Set new group_id as the current length of the DataFrame + 1
                    copyRow['appAprrovedby'] = int(group_id)
                    copyRow['appAction'] = 'Pending'
                    # old_row = self.df[self.df['id'] == group_id].iloc[0]
                    self.df.loc[len(self.df)] = copyRow
                    group_id = copyRow['id']
                    # self.save_to_database(self.df)
                    old_values = {
                        'group_id': old_row['GroupId'],
                        'group_name': old_row['GroupName'],
                        'status': old_row['Status'],
                        'appstatus': old_row['Appstatus'],
                        'modified_by': old_row['modified_by']
                    }
                    if group_id:
                        self.df.loc[self.df['id'] == group_id, 'GroupId'] = group_id
                    if GroupName:
                        self.df.loc[self.df['id'] == group_id, 'GroupName'] = GroupName
                    if Status:
                        self.df.loc[self.df['id'] == group_id, 'Status'] = Status
                    if appstatus:
                        self.df.loc[self.df['id'] == group_id, 'appstatus'] = appstatus
                    if modified_by:
                        self.df.loc[self.df['id'] == group_id, 'modified_by'] = modified_by

                    self.df.loc[self.df['id'] == group_id, 'reservedfield1'] = str(old_values)

                    userdetails = session.get('userdetails')

                    new_row = self.df[self.df['id'] == group_id].iloc[0]
                    new_values = {
                        'group_id': new_row['GroupId'],
                        'group_name': new_row['GroupName'],
                        'status': new_row['Status'],
                        'appstatus': new_row['Appstatus'],
                        'modified_by': new_row['modified_by']
                    }
                    self.save_to_database(self.df.loc[self.df['id'] == group_id], 'insert')
                    log_event(f'log_{int(time.time())}', userdetails.get('UserName'), f'User updated from {old_values} to {new_values}', f'/Group_Management_module/GroupManagement',
                    f'user_{group_id}', session['bankid'], "Maker", self.conn)

                    return group_id

        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_number = tb[-1].lineno  # Get the line number of the exception
            file_name = tb[-1].filename  # Get the file name
            method_name = tb[-1].name
            log_error_to_database(
                user_id=session['user1'],
                machine_ip=request.remote_addr,
                description=str(e),
                upload_filename=file_name,  # If applicable
                line_no=line_number,
                method_name=method_name,
                upload_by="System"
            )
        return render_template('error.html', userdetails=session.get('userdetails'), error=str(e))

    def delete_group(self, group_id, username, appstatus=None, case=None):
        try:
            group_id = str(group_id)

            if appstatus == 'Approved':
                copy_row = self.df[self.df['id'] == group_id]
                original_id = copy_row['appAprrovedby'].values[0]
                self.df = self.df[~self.df['id'].isin([group_id, original_id])]
                userdetails = session.get('userdetails')
                self.save_to_database(self.df[self.df['id'].isin([group_id, original_id])], 'delete',
                                      [group_id, original_id])
                log_event(f'log_{int(time.time())}', userdetails.get('UserName'), f'User_deleted',
                          '/Group_Management_module/GroupManagement', f'role_{group_id}', session['bankid'], "Maker", self.conn)
            else:
                old_row = self.df[self.df['id'] == group_id].iloc[0]
                copyRow = old_row.copy()
                copyRow['id'] = int(time.time())
                copyRow['appAprrovedby'] = group_id
                copyRow['appAction'] = 'Pending'
                self.df.loc[len(self.df)] = copyRow
                new_id = copyRow['id']

            if group_id in self.df['id'].values:
                self.df.loc[self.df['id'] == new_id, 'appAprrovedby'] = group_id
                self.df.loc[self.df['id'] == new_id, 'reservedfield1'] = 'delete'

                role = self.df[self.df['id'] == group_id]

                if role['UserName'].values[0] == username:
                    self.df = self.df[self.df['id'] != group_id]
                    return 'redirect_login'

                userdetails = session.get('userdetails')
                self.save_to_database(self.df[self.df['id'] == new_id], 'insert', new_id)
                log_event(f'log_{int(time.time())}', userdetails.get('UserName'), f'User deleted',
                          '/Group_Management_module/GroupManagement', f'group_{group_id}', session['bankid'], "Maker", self.conn)

        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_number = tb[-1].lineno
            file_name = tb[-1].filename
            method_name = tb[-1].name
            log_error_to_database(
                user_id=session['user1'],
                machine_ip=request.remote_addr,
                description=str(e),
                upload_filename=file_name,
                line_no=line_number,
                method_name=method_name,
                upload_by="System"
            )
        return render_template('error.html', userdetails=session.get('userdetails'), error=str(e))

    def toggle_group_status(self, group_id, appstatus=None):
        try:
            group_id = str(group_id)
            copy_row = self.df[self.df['id'] == group_id]

            if not copy_row.empty:
                original_id = copy_row['appAprrovedby'].values[0]
                current_status = self.df.loc[self.df['id'] == original_id, 'Status'].iloc[0]
                new_status = 'Active' if current_status == 'Inactive' else 'Inactive'
                self.df.loc[self.df['id'] == original_id, 'Status'] = new_status
                self.save_to_database(self.df.loc[self.df['id'] == original_id], 'update', original_id)
                self.save_to_database(copy_row, 'delete', group_id)
                self.df = self.df[~self.df['id'].isin([int(group_id)])]
            else:
                old_row = self.df[self.df['id'] == group_id].iloc[0]
                copy_row = old_row.copy()
                copy_row['id'] = int(time.time())
                copy_row['appAprrovedby'] = group_id
                copy_row['appAction'] = 'Pending'
                self.df.loc[len(self.df)] = copy_row
                new_id = copy_row['id']
                if group_id in self.df['id'].values:
                    self.df.loc[self.df['id'] == new_id, 'appAprrovedby'] = group_id
                    self.df.loc[self.df['id'] == new_id, 'reservedfield1'] = 'toggle'
                    current_status = self.df.loc[self.df['id'] == group_id, 'Status'].iloc[0]
                    new_status = 'Active' if current_status == 'Inactive' else 'Inactive'
                    self.df.loc[self.df['id'] == new_id, 'Status'] = new_status

                    userdetails = session.get('userdetails')

                    self.save_to_database(self.df[self.df['id'] == new_id], 'insert', new_id)

                    log_event(f'log_{int(time.time())}', userdetails.get('UserName'),
                              f'Sent request to change status from {current_status} to {new_status}',
                              '/Group_Management_module/GroupManagement',
                              f'role_{group_id}', session['bankid'], "Maker", self.conn)

            return 'Enabled'
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_number = tb[-1].lineno  # Get the line number of the exception
            file_name = tb[-1].filename  # Get the file name
            method_name = tb[-1].name
            log_error_to_database(
                user_id=session['user1'],
                machine_ip=request.remote_addr,
                description=str(e),
                upload_filename=file_name,  # If applicable
                line_no=line_number,
                method_name=method_name,
                upload_by="System"
            )
            return render_template('error.html', userdetails=session.get('userdetails'), error=str(e))


    def get_groups(self):
        # engine = create_engine(config["engine"])
        userdetails = session.get('userdetails')
        # Connect to the database
        query = f"SELECT * FROM GroupMaster WHERE bankid = '{userdetails['bankid']}'"
        with get_SQL_engine().connect() as connection:
            self.df = pd.read_sql(query, con=connection)
        return self.df