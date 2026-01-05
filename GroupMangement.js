let currentPage = 0;
document.addEventListener("DOMContentLoaded", function () {
    let addGroupButton = document.getElementById("add-group-button");
    const newRoleRow = document.getElementById("new-group-row");
    const addRoleBtn = newRoleRow.querySelector(".add-group-btn");
    const approveButton = document.getElementById('approve-button');
    const pendingButton = document.getElementById('pending-button');
    const declineButton = document.getElementById('decline-button');

    function setActiveButton(button) {
        button.classList.add('btn-active');
    }

// ------------------------------------------------------------------------------------------ Page Shift in rule page -------------------------------------------------------------------------------

    approveButton.addEventListener('click', function () {
        setActiveButton(approveButton);
        window.location.href = '/Group_Management_module/GroupManagement';
    });

    pendingButton.addEventListener('click', function () {
        setActiveButton(pendingButton);
        window.location.href = '/Group_Management_module/GroupManagement/pending';
    });

    declineButton.addEventListener('click', function () {
        setActiveButton(declineButton);
        window.location.href = '/Group_Management_module/GroupManagement/declined';
    });
// ------------------------------------------------------------------------------------------ Set the active button based on the current URL -------------------------------------------------------------------------------
    const currentUrl = window.location.pathname;
    if (currentUrl.endsWith('/pending')) {
        setActiveButton(pendingButton);
    }
    else if (currentUrl.endsWith('/declined')) {
        setActiveButton(declineButton);
    }
    else {
        setActiveButton(approveButton);
    }


    document.querySelectorAll('.delete-group-btn').forEach(function(button) {
        button.addEventListener('click', function(event) {
            event.preventDefault();
            if (confirm('Are you sure you want to delete this group?')) {

                this.closest('form').submit();

            }
            else{
                location.reload();
            }
        });
    });
    document.querySelectorAll('.toggle-group-btn').forEach(function(button) {
        button.addEventListener('click', function(event) {
            event.preventDefault();
            const row = button.closest("tr");
            const status = row.querySelector(".Status-value").textContent.trim();
            const confirmationMessage = status === "Active" ?
                "Are you sure you want to make this role Inactive?" :
                "Are you sure you want to make this role Active?";
            if (confirm(confirmationMessage)) {
                this.closest('form').submit();
            }
           else{
                location.reload();
           }

        });
    });
    // Event listener for Add Role button
    addGroupButton.addEventListener("click", function () {
        newGroupRow.style.display = "table-row";
//        addGroupButton.style.marginRight = "800px";
//        addGroupButton.style.display = "none";
          addGroupButton.type = "hidden";
    });

    // Event listener for Add Role button inside the new role row
    addGroupBtn.addEventListener("click", function () {

        const GroupName = document.querySelector("select[name='new-GroupName']").value;
//        const bank = document.querySelector(".form-control[name='new-bank']").value;
        const status = document.querySelector("select[name='new-status']").value;

       addGroupButton.addEventListener("click", function () {
        newGroupRow.style.display = "table-row";
//        addGroupButton.style.marginRight = "800px";
//        addGroupButton.style.display = "none";
          addGroupButton.type = "hidden";
    });

    // Event listener for Add Role button inside the new role row
    addGroupBtn.addEventListener("click", function () {

        const GroupName = document.querySelector("select[name='new-GroupName']").value;
//        const bank = document.querySelector(".form-control[name='new-bank']").value;
        const status = document.querySelector("select[name='new-status']").value;

    // Perform validation
    let GroupName = document.querySelector("input[name='new-groupname']").value;
        //        let bankval = bank
    //        if (!bankval){
    //            alert("Please select Bank");
    //        }
    //        else{
    //        if (message2=='Done' && message3=='Done' && message4=='Done' && bank) {
    let status = document.querySelector("select[name='new-status']").value;

    if (GroupName && status) {

                const formData = new FormData();

                formData.append("GroupName", GroupName);
    //            formData.append("bankid", bank);
                formData.append("Status", status);
                formData.append("username", "{{ user }}");

        fetch("/Group_Management_module/GroupManagement", {
            method: "POST",
            body: formData,
        })
        .then((response) => {
            if (response.ok) {
                alert("Group Added Successfully!!!");
            } else if (response.status === 409) {
                alert("Group Already Exist!!!!!");
            } else {
                throw new Error("Failed to add group");
            }
        })
        .then((data) => {
            if (data) {
                location.reload();
            }
        });

        newGroupRow.style.display = "none";
        addGroupButton.style.display = "block";

    }
    else {

        if (!GroupName) {
            alert("Please enter Group Name");
        }

        if (!status) {
            alert("Please select Status");
        }

        location.reload();
    }


    const dropdown = document.getElementById("statusSelect");
    const clearTd = document.getElementById("clearTd");

    dropdown.addEventListener("change", function () {
        const RoleName = dropdown.value;
        clearTd.innerHTML = "";

    });

    const editButtons = document.querySelectorAll(".edit-btn");
    editButtons.forEach(function (button) {
        button.addEventListener("click", function () {
            const row = button.closest("tr");

        // View (span) elements
        const groupNameValue = row.querySelector(".GroupName-value");
        const statusValue = row.querySelector(".Status-value");

        // Edit (input/select) elements
        const groupNameEdit = row.querySelector(".groupname-edit");
        const statusEdit = row.querySelector(".status-edit");
        const updateBtn = row.querySelector(".update-btn");

        // Copy existing values into edit fields
        groupNameEdit.value = groupNameValue.textContent.trim();
        statusEdit.value = statusValue.textContent;

        // Hide view mode
        groupNameValue.style.display = "none";
        statusValue.style.display = "none";
        button.style.display = "none";

        // Show edit mode
        groupNameEdit.style.display = "block";
        statusEdit.style.display = "block";
        updateBtn.style.display = "inline-block";
    });
});

    const updateButtons = document.querySelectorAll(".update-btn");

    updateButtons.forEach(function (button) {
        button.addEventListener("click", function (event) {
            event.preventDefault();

            if (confirm("Are you sure you want to update this group?")) {
                const row = button.closest("tr");

                const groupNameEdit = row.querySelector(".groupname-edit");
                const statusEdit = row.querySelector(".status-edit");

                const formData = new FormData();
                formData.append("GroupName", groupNameEdit.value);
                formData.append("Status", statusEdit.value);
                formData.append("username", "{{ user }}");

                fetch("/Group_Management_module/GroupManagement/update/" + row.id.split("-")[1], {
                    method: "POST",
                    body: formData
                })
                .then(response => {
                    if (response.ok) {
                        alert("Changes Saved Successfully!!");
                        location.reload();
                    } else {
                        console.error("Failed to update group");
                    }
                })
                .catch(error => {
                    console.error("Error:", error);
                });
            }
        });
    });


    const objid = getQueryParam('objid');
        if (objid) {
            const row = document.getElementById(`role-${objid}`);
            if (row) {
                row.classList.add('blink-row');
                const tbody = row.closest('tbody');
                const tbodyId = (tbody.id).match(/\d+/)[0];
                currentPage = parseInt(tbodyId);
            }
        }


function getQueryParam(param) {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get(param);
}


function validateGroupName(ID) {
    let name = document.getElementById(ID).value;
    let errors = [];

    if (name.length > 90) {
        errors.push("not exceed 90 characters");
    }
    if (name.length < 3) {
        errors.push("be at least 3 characters long");
    }
    if (!/^[a-zA-Z0-9_.]+$/.test(name)) {
        errors.push("contain only letters, numbers, underscores, or dots");
    }

    if (errors.length > 0) {
        if (errors.length === 1) {
            return `Your group name must ${errors[0]}.`;
        } else {
            return `Your group name must ${errors.slice(0, -1).join(', ')}, and ${errors[errors.length - 1]}.`;
        }
    }
    return "Done";
}

