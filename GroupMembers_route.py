from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
import pyodbc
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.testing import db
from app.GetOptionsFromDB import get_options_from_db
from app.GroupManagement import GroupManagementManager
import pandas as pd
from app.LogsImport import log_error_to_database
from datetime import time, datetime
from app.Configuration import config
from app.alert_route import inAlert, upAlert
from app.FRMDBOperations import *

GroupModel_route = Blueprint('GroupModel_route', __name__)

# conn = pyodbc.connect(
#             'DRIVER='+config["driver"]+';SERVER=' + config["server"] + ';DATABASE=' + config["database"] + ';UID=' + config["username"] + ';PWD=' + config["password"])
GroupMembersdf=pd.read_sql("SELECT * FROM tbl_GroupMembers",con=get_SQL_connection())

userdata=pd.read_sql('''EXEC GetUserDataWithUserType ''',get_SQL_connection())

GroupMembers_manager = GroupManagementManager(None,GroupMembersdf,get_SQL_engine(),get_SQL_connection())

# def extract_search_parameters(filtered_roles_data):
#     search_bankid = request.form.get('search_bankid')
#     search_status = request.form.get('search_status')
#     if search_bankid:
#         filtered_roles_data = filtered_roles_data[filtered_roles_data['bankid'].astype(str).str.contains(search_bankid.strip())]
#     if search_status != None and search_status != '':
#         filtered_roles_data = filtered_roles_data[
#             filtered_roles_data['Status'].str.strip().str.lower() == search_status.strip().lower()
#             ]
#     return filtered_roles_data


@GroupModel_route.route('/Accountblock/GroupManagement', defaults={'subpath': ''}, methods=['GET', 'POST'])
@GroupModel_route.route('/Group_Management_module/GroupManagement/<path:subpath>', methods=['GET', 'POST'])
def group_manager(subpath):
    try:
        username = session.get('username')
        userdetails = session.get('userdetails')
        if not username:
            return redirect(url_for('LoginPage'))

            group_members_data = GroupMembers_manager.get_group_members()

        if request.method == 'POST' and subpath == '':
            bankid = session.get('bankid')
            cif = request.form.get('CIF')            # CIF from form
            group_id = request.form.get('GroupId')   # GroupId from form
            app_action = request.form.get('appAction')  # action/status from form
            created_by = username

            # Insert new group member
            message = GroupMembers_manager.create_group(
                CIF=cif,
                GroupId=group_id,
                bankid=bankid,
                appAction=app_action,
                created_by=created_by
            )

            # Get checkers for alert
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()

            # Send alert
            inAlert(
                {
                    'AlertCategory': 'GroupMember_Addition',
                    'NavLink': '/Group_Management_module/GroupManagement/pending',
                    'Description': f"{username} (Maker) added a Group Member: CIF {cif} in Group {group_id}",
                    'is_seen': 0,
                    'to_whom': to_whom_ids,
                    'ObjId': message
                },
                'groupmember',
                ModuleIndex=18
            )

            return redirect(url_for('GroupModel_route.group_manager', subpath='pending'))


        elif subpath == 'pending':
            pending_members = group_members_data[group_members_data['appAction'] == 'Pending'].to_dict('records')
            return render_template(
                'GroupMembers_ro.html',
                group_members=pending_members,
                action=subpath,
                user=username,
                userdetails=userdetails
            )

        elif subpath == 'approved':
            approved_members = group_members_data[group_members_data['appAction'] == 'Approved'].to_dict('records')
            return render_template(
                'GroupManagement.html',
                group_members=approved_members,
                action=subpath,
                user=username,
                userdetails=userdetails
            )

        elif subpath == 'declined':
            declined_members = group_members_data[group_members_data['appAction'] == 'Declined'].to_dict('records')
            return render_template(
                'GroupManagement.html',
                group_members=declined_members,
                action=subpath,
                user=username,
                userdetails=userdetails
            )

        else:
            approved_members = group_members_data[group_members_data['appAction'] == 'Approved'].to_dict('records')
            return render_template(
                'GroupManagement.html',
                group_members=approved_members,
                action='',
                user=username,
                userdetails=userdetails
            )

        return render_template('GroupManagement.html', groups=approved_groups, action='', user=username,
                               userdetails=userdetails)

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

@GroupModel_route.route('/Group_Management_module/GroupManagement/update/<int:member_id>', methods=['POST'])
def update_group(group_id):
    try:
        username = session.get('username')
        userdetails = session.get('userdetails')
        if not username:
            return redirect(url_for('LoginPage'))

        if 'action' in request.form and request.form['action'] == 'approved':

            UserforSending = GroupMembers_manager.df.loc[GroupMembers_manager.df['id'] == group_id, 'created_by'].values[0] \
                if GroupMembers_manager.df.loc[GroupMembers_manager.df['id'] == group_id, 'created_by'].size > 0 else None

            grp_id = GroupMembers_manager.update_member(group_id=group_id, appAction='Approved', modified_by=username)

            inAlert(
                {
                    'AlertCategory': 'GroupMember_Approved',
                    'NavLink': '/Group_Management_module/GroupManagement/approved',
                    'Description': f"{username} (Approver) approved the Group Member",
                    'is_seen': 0,
                    'to_whom': UserforSending,
                    'ObjId': grp_id
                },
                'personal'
            )
            return redirect(url_for('GroupModel_route.group_manager', subpath='approved'))

        # ---- DECLINE ----
        elif 'action' in request.form and request.form['action'] == 'declined':
            UserforSending = GroupMembers_manager.df.loc[GroupMembers_manager.df['id'] == group_id, 'created_by'].values[0] \
                if GroupMembers_manager.df.loc[GroupMembers_manager.df['id'] == group_id, 'created_by'].size > 0 else None

            grp_id = GroupMembers_manager.update_group(group_id=group_id, appAction='Declined', modified_by=username)

            inAlert(
                {
                    'AlertCategory': 'GroupMember_Declined',
                    'NavLink': '/Group_Management_module/GroupManagement/declined',
                    'Description': f"{username} (Approver) declined the Group Member",
                    'is_seen': 0,
                    'to_whom': UserforSending,
                    'ObjId': grp_id
                },
                'personal'
            )
            return redirect(url_for('GroupModel_route.group_manager', subpath='declined'))

        # ---- RE-ALERT ----
        elif 'action' in request.form and request.form['action'] == 'alert':
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
            errmsg = inAlert(
                {
                    'AlertCategory': 'GroupMember_Alert',
                    'NavLink': '/Group_Management_module/GroupManagement/pending',
                    'Description': f"{username} (Maker) re-alerted the Group Member",
                    'is_seen': 0,
                    'to_whom': to_whom_ids,
                    'ObjId': group_id
                },
                'groupmember',
                ModuleIndex=18
            )

            return redirect(url_for('GroupModel_route.group_manager', subpath='pending', erromsg=errmsg))

        # ---- REQUEST AGAIN ----
        elif 'action' in request.form and request.form['action'] == 'RequestAgain':
            grp_id = GroupMembers_manager.update_group(group_id=group_id, appAction='Pending', modified_by=username)
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
            inAlert(
                {
                    'AlertCategory': 'GroupMember_Resent',
                    'NavLink': '/Group_Management_module/GroupManagement/pending',
                    'Description': f"{username} (Maker) resent the Group Member",
                    'is_seen': 0,
                    'to_whom': to_whom_ids,
                    'ObjId': grp_id
                },
                'groupmember',
                ModuleIndex=18
            )
            return redirect(url_for('GroupModel_route.group_manager', subpath='pending'))

        # ---- UPDATE DETAILS ----
        else:
            cif = request.form.get('CIF')
            group_id_form = request.form.get('GroupId')
            reservedfield1 = request.form.get('reservedfield1')
            to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()

            GroupMembers_manager.update_group(
                group_id=group_id,
                CIF=cif,
                GroupId=group_id_form,
                reservedfield1=reservedfield1,
                appAction='Pending',
                modified_by=username
            )

            inAlert(
                {
                    'AlertCategory': 'GroupMember_Updated',
                    'NavLink': '/Group_Management_module/GroupManagement/pending',
                    'Description': f"{username} updated the Group Member",
                    'is_seen': 0,
                    'to_whom': to_whom_ids,
                    'ObjId': group_id
                },
                'groupmember',
                ModuleIndex=18
            )
            return redirect(url_for('GroupModel_route.group_manager', subpath='pending'))

    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        line_number = tb[-1].lineno
        file_name = tb[-1].filename
        method_name = tb[-1].name
        log_error_to_database(
            user_id=session.get('user1'),
            machine_ip=request.remote_addr,
            description=str(e),
            upload_filename=file_name,
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
                grp_id = GroupMembers_manager.delete_group(group_id=group_id, username=session.get('username'),
                                                       appstatus='Approved')
                return redirect(url_for('GroupManagement_route.GroupManagement', subpath='approved', user=username))

            else:
                result = GroupMembers_manager.delete_group(group_id, username)
                if result == 'redirect_login':
                    session.clear()
                    return redirect(url_for('LoginPage'))
                elif result == 'success':
                    return redirect(url_for('GroupManagement_route.GroupManagement', user=username))
                else:

                    inAlert({'AlertCategory': 'Group_Deletion',
                             'NavLink': '/Group_Management_module/GroupManagement/approved',
                             'Description': username + ' deleted the group', 'is_seen': 0,
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
                grp_id = GroupMembers_manager.toggle_group_status(group_id=group_id, appstatus='Approved')

                # currStatus = Group_Management.toggle_role_status(group_id)
                to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
                inAlert({'AlertCategory': f'User_{group_id.lower()}',
                         'NavLink': '/Group_Management_module/GroupManagement/approved',
                         'Description': username + ' change status of User', 'is_seen': 0,
                         'to_whom': to_whom_ids,
                         'ObjId': group_id}, 'group', ModuleIndex=18)
                return redirect(url_for('GroupManagement_route.GroupManagement', subpath='approved', user=username))
            else:
                # Group_Management.toggle_role_status(group_id,appstatus=None)
                currStatus = GroupMembers_manager.toggle_group_status(group_id, appstatus=None)
                to_whom_ids = userdata[userdata['UserType'] == 'Checker']['RoleID'].tolist()
                inAlert({'AlertCategory': f'User_{currStatus.lower()}',
                         'NavLink': '/Group_Management_module/GroupManagement/approved',
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














