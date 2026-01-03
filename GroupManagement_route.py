from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
import pyodbc
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.testing import db
from app.GetOptionsFromDB import get_options_from_db
from app.GroupManagement import GroupManager
import pandas as pd
from app.LogsImport import log_error_to_database
from datetime import time, datetime
from app.Configuration import config
from app.alert_route import inAlert, upAlert
from app.FRMDBOperations import *

GroupManagement_route = Blueprint('GroupManagement_route', __name__)

# conn = pyodbc.connect(
#             'DRIVER='+config["driver"]+';SERVER=' + config["server"] + ';DATABASE=' + config["database"] + ';UID=' + config["username"] + ';PWD=' + config["password"])
GroupMasterdf=pd.read_sql("SELECT * FROM GroupMaster",con=get_SQL_connection())

userdata=pd.read_sql('''EXEC GetUserDataWithUserType ''',get_SQL_connection())
GroupMembersdf = pd.read_sql("SELECT * FROM tbl_GroupMembers", get_SQL_connection())

Group_Management = GroupManager(None,GroupMasterdf,GroupMembersdf,get_SQL_engine(),get_SQL_connection())

# def extract_search_parameters(filtered_roles_data):
#     search_bankid = request.form.get('search_bankid')
#     search_channel = request.form.get('search_channel')
#     search_mcc = request.form.get('search_mcc')
#     search_status = request.form.get('search_status')
#     if search_bankid:
#         filtered_roles_data = filtered_roles_data[filtered_roles_data['bankid'].astype(str).str.contains(search_bankid.strip())]
#     if search_channel != None and search_channel != '':
#         filtered_roles_data = filtered_roles_data[
#             filtered_roles_data['BlockedChannels'].str.contains(search_channel.strip(), case=False)]
#     if search_mcc != None and search_mcc != '':
#         filtered_roles_data = filtered_roles_data[
#             filtered_roles_data['BlockedMCCs'].str.contains(search_mcc.strip(), case=False)]
#     if search_status != None and search_status != '':
#         filtered_roles_data = filtered_roles_data[
#             filtered_roles_data['Status'].str.strip().str.lower() == search_status.strip().lower()
#             ]
#     return filtered_roles_data

@GroupManagement_route.route('/Group_Management_module/GroupManagement', defaults={'subpath': ''}, methods=['GET', 'POST'])
@GroupManagement_route.route('/Group_Management_module/GroupManagement/<path:subpath>', methods=['GET', 'POST'])
def GroupManagement(subpath):
    try:
        username = session.get('username')
        userdetails = session.get('userdetails')
        if not username:
            return redirect(url_for('LoginPage'))

        groups_df = Group_Management.get_groups()
        # groups_df = extract_search_parameters(groups_df)

        if request.method == 'POST' and subpath == '':
            group_name = request.form.get('GroupName')
            status = request.form.get('Status')
            bankid = session.get('bankid')
            created_by = username


            message = Group_Management.create_group(group_name=group_name, bankid=bankid, Status=status, created_by=created_by)
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
            inAlert(
                {
                    'AlertCategory': 'Group_Addition',
                    'NavLink': '/Group_Management_module/GroupManagement/pending',
                    'Description': f"{username} (Maker) added a Group",
                    'is_seen': 0,
                    'to_whom': to_whom_ids,
                    'ObjId': message
                },
                'group',
                ModuleIndex=18
            )

            return redirect(url_for('GroupManagement_route.GroupManagement', subpath='pending'))

            # ---- PENDING GROUPS ----
        if subpath == 'pending':
            pending_groups = groups_df[groups_df['appAction'] == 'Pending'].to_dict('records')
            return render_template(
                'GroupManagement.html',
                groups=pending_groups,
                action=subpath,
                user=username,
                userdetails=userdetails
            )

        # ---- APPROVED GROUPS ----
        elif subpath == 'approved':
            approved_groups = groups_df[groups_df['appAction'] == 'Approved'].to_dict('records')
            return render_template(
                'GroupManagement.html',
                groups=approved_groups,
                action=subpath,
                user=username,
                userdetails=userdetails
            )

        # ---- DECLINED GROUPS ----
        elif subpath == 'declined':
            declined_groups = groups_df[groups_df['appAction'] == 'Declined'].to_dict('records')
            return render_template(
                'GroupManagement.html',
                groups=declined_groups,
                action=subpath,
                user=username,
                userdetails=userdetails
            )

        else:
            approved_groups = groups_df[groups_df['appAction'] == 'Approved'].to_dict('records')
            return render_template('GroupManagement.html', groups=approved_groups, action='', user=username, userdetails=userdetails)

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


@GroupManagement_route.route('/Group_Management_module/GroupManagement/update/<int:group_id>', methods=['POST'])
def update_group(group_id):
    try:
        username = session.get('username')
        userdetails = session.get('userdetails')

        if not username:
            return redirect(url_for('LoginPage'))
        if 'action' in request.form and request.form['action'] == 'approved':
                grp_id = Group_Management.update_group(group_id=group_id, appstatus='Approved', modified_by=username)
                UserforSending = Group_Management.df.loc[Group_Management.df['id'] == group_id, 'created_by'].values[0] if \
                    Group_Management.df.loc[Group_Management.df['id'] == group_id, 'created_by'].size > 0 else None
                inAlert({'AlertCategory': 'User_Approved', 'NavLink': '/Group_Management_module/GroupManagement/approved',
                         'Description': username + ' (Approver) approved the User', 'is_seen': 0,
                         'to_whom': UserforSending,
                         'ObjId': grp_id}, 'personal')
                return redirect(url_for('GroupManagement_route.GroupManagement', subpath='approved', user=username))

        elif 'action' in request.form and request.form['action'] == 'declined':
            grp_id = Group_Management.update_group(group_id=group_id, appstatus='Declined', modified_by=username)
            UserforSending = Group_Management.df.loc[Group_Management.df['id'] == group_id, 'created_by'].values[0] \
                if Group_Management.df.loc[Group_Management.df['id'] == group_id, 'created_by'].size > 0 else None
            inAlert({'AlertCategory': 'User_Declined', 'NavLink': '/Group_Management_module/GroupManagement/declined',
                     'Description': username + '(Approver) declined the rule', 'is_seen': 0,
                     'to_whom': UserforSending,
                     'ObjId': grp_id}, 'personal')
            return redirect(url_for('GroupManagement_route.GroupManagement', subpath='declined', user=username))
        elif 'action' in request.form and request.form['action'] == 'alert':
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
            errmsg = inAlert({'AlertCategory': 'User_Alert', 'NavLink': '/Group_Management_module/GroupManagement/pending',
                              'Description': username + ' (Maker) re-alert the User', 'is_seen': 0,
                              'to_whom': to_whom_ids,
                              'ObjId': group_id}, 'group',ModuleIndex=18)
            if errmsg:
                return redirect(url_for('GroupManagement_route.GroupManagement', subpath='pending', user=username, erromsg=errmsg))
            return redirect(url_for('GroupManagement_route.GroupManagement', subpath='pending', user=username))
        elif 'action' in request.form and request.form['action'] == 'RequestAgain':
            grp_id = Group_Management.update_group(group_id=group_id, appstatus='Pending', modified_by=username)
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
            inAlert({'AlertCategory': 'User_Resent', 'NavLink': '/Group_Management_module/GroupManagement/pending',
                     'Description': username + ' (Maker) resent the User', 'is_seen': 0,
                     'to_whom': to_whom_ids,
                     'ObjId': grp_id}, 'group',ModuleIndex=18)
            return redirect(url_for('GroupManagement_route.GroupManagement', subpath='declined', user=username))
        else:
            GroupName = request.form.get('GroupName')
            Status = request.form.get('Status')
            reservedfield1 = request.form.get('reservedfield1')
            userdetails = session.get('userdetails')
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
            Group_Management.update_group(group_id=group_id, group_name=GroupName, status=Status, reservedfield1=reservedfield1, appstatus='Pending', modified_by=username)

            inAlert({'AlertCategory': 'User_Updated', 'NavLink': '/Group_Management_module/GroupManagement/approved',
                     'Description': username + ' updated the User', 'is_seen': 0,
                     'to_whom': to_whom_ids,
                     'ObjId': group_id}, 'group',ModuleIndex=18)
            return redirect(url_for('GroupManagement_route.GroupManagement', subpath='pending', user=username))

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


@GroupManagement_route.route('/Group_Management_module/GroupManagement/delete/<int:group_id>', methods=['POST'])
def delete_role(group_id):
            try:
                username = session.get('username')
                if not username:
                    return redirect(url_for('LoginPage'))
                if 'action' in request.form and request.form['action'] == 'approved':
                    rule_id = Group_Management.delete_group(group_id=group_id, username=session.get('username'),
                                                       appstatus='Approved')
                    return redirect(url_for('GroupManagement_route.GroupManagement', subpath='approved', user=username))

                else:
                    result = Group_Management.delete_group(group_id, username)
                    if result == 'redirect_login':
                        session.clear()
                        return redirect(url_for('LoginPage'))
                    elif result == 'success':
                        return redirect(url_for('GroupManagement_route.GroupManagement', user=username))
                    else:

                        inAlert({'AlertCategory': 'User_Deletion', 'NavLink': '/Group_Management_module/GroupManagement/approved',
                                 # create a page to display deleted rules.
                                 'Description': username + ' deleted the User', 'is_seen': 0,
                                 'to_whom': userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist(),
                                 'ObjId': group_id}, 'group', ModuleIndex=19)

                return redirect(url_for('GroupManagement_route.GroupManagement', user=username))

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

@GroupManagement_route.route('/Group_Management_module/GroupManagement/toggle/<int:group_id>', methods=['POST'])
def toggle_role(group_id):
    try:
        username = session.get('username')
        if 'action' in request.form and request.form['action'] == 'approved':
            rule_id = Group_Management.toggle_role_status(group_id=group_id, appstatus='Approved')

            # currStatus = Group_Management.toggle_role_status(group_id)
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
            inAlert({'AlertCategory': f'User_{rule_id.lower()}', 'NavLink': '/Group_Management_module/GroupManagement/approved',
                     'Description': username + ' change status of User', 'is_seen': 0,
                     'to_whom': to_whom_ids,
                     'ObjId': rule_id}, 'group', ModuleIndex=18)
            return redirect(url_for('GroupManagement_route.GroupManagement', subpath='approved', user=username))
        else:
            # Group_Management.toggle_role_status(group_id,appstatus=None)
            currStatus = Group_Management.toggle_role_status(group_id, appstatus=None)
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
            inAlert({'AlertCategory': f'User_{currStatus.lower()}', 'NavLink': '/Group_Management_module/GroupManagement/approved',
                     'Description': username + ' change status of User', 'is_seen': 0,
                     'to_whom': to_whom_ids,
                     'ObjId': group_id}, 'group', ModuleIndex=18)
            return redirect(url_for('GroupManagement_route.GroupManagement'))
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







