let currentPage = 0;

document.addEventListener("DOMContentLoaded", function () {

    let addGroupButton = document.getElementById("add-group-button");
    const newGroupRow = document.getElementById("new-group-row");
    const addGroupBtn = newGroupRow.querySelector(".add-group-btn");

    const approveButton = document.getElementById('approve-button');
    const pendingButton = document.getElementById('pending-button');
    const declineButton = document.getElementById('decline-button');

    function setActiveButton(button) {
        button.classList.add('btn-active');
    }

    // --------------------------------------------------------- Page Shift in rule page -------------------------------------------------------------------------------

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

    // --------------------------------------------------------- Set active button based on URL -----------------------------------------------------------------------

    const currentUrl = window.location.pathname;
    if (currentUrl.endsWith('/pending')) {
        setActiveButton(pendingButton);
    } else if (currentUrl.endsWith('/declined')) {
        setActiveButton(declineButton);
    } else {
        setActiveButton(approveButton);
    }

    // --------------------------------------------------------- Delete Group -----------------------------------------------------------------------

    document.querySelectorAll('.delete-group-btn').forEach(function (button) {
        button.addEventListener('click', function (event) {
            event.preventDefault();
            if (confirm('Are you sure you want to delete this group?')) {
                this.closest('form').submit();
            } else {
                location.reload();
            }
        });
    });

    // --------------------------------------------------------- Toggle Group Status -----------------------------------------------------------------------

    document.querySelectorAll('.toggle-group-btn').forEach(function (button) {
        button.addEventListener('click', function (event) {
            event.preventDefault();

            const row = button.closest("tr");
            const status = row.querySelector(".Status-value").textContent.trim();

            const confirmationMessage =
                status === "Active"
                    ? "Are you sure you want to make this role Inactive?"
                    : "Are you sure you want to make this role Active?";

            if (confirm(confirmationMessage)) {
                this.closest('form').submit();
            } else {
                location.reload();
            }
        });
    });

    // --------------------------------------------------------- Add Group (Show Row) -----------------------------------------------------------------------

    addGroupButton.addEventListener("click", function () {
        newGroupRow.style.display = "table-row";
        addGroupButton.style.display = "none";
    });

    // --------------------------------------------------------- Add Group (Submit) -----------------------------------------------------------------------

    addGroupBtn.addEventListener("click", function () {

        let GroupName = document.querySelector("input[name='new-groupname']").value.trim();
        let status = document.querySelector("select[name='new-status']").value;

        if (!GroupName) {
            alert("Please enter Group Name");
            return;
        }

        if (!status) {
            alert("Please select Status");
            return;
        }

        const formData = new FormData();
        formData.append("GroupName", GroupName);
        formData.append("Status", status);
        formData.append("username", "{{ user }}");

        fetch("/Group_Management_module/GroupManagement", {
            method: "POST",
            body: formData
        })
        .then(response => {
            if (response.ok) {
                alert("Group Added Successfully!!!");
                location.reload();
            } else if (response.status === 409) {
                alert("Group Already Exist!!!!!");
            } else {
                throw new Error("Failed to add group");
            }
        })
        .catch(error => console.error("Error:", error));
    });

    // --------------------------------------------------------- Edit Group -----------------------------------------------------------------------

    const editButtons = document.querySelectorAll(".edit-btn");

    editButtons.forEach(function (button) {
        button.addEventListener("click", function () {

            const row = button.closest("tr");

            const groupNameValue = row.querySelector(".GroupName-value");
            const statusValue = row.querySelector(".Status-value");

            const groupNameEdit = row.querySelector(".groupname-edit");
            const statusEdit = row.querySelector(".status-edit");
            const updateBtn = row.querySelector(".update-btn");

            groupNameEdit.value = groupNameValue.textContent.trim();
            statusEdit.value = statusValue.textContent;

            groupNameValue.style.display = "none";
            statusValue.style.display = "none";
            button.style.display = "none";

            groupNameEdit.style.display = "block";
            statusEdit.style.display = "block";
            updateBtn.style.display = "inline-block";
        });
    });

    // --------------------------------------------------------- Update Group -----------------------------------------------------------------------

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
                .catch(error => console.error("Error:", error));
            }
        });
    });

    // --------------------------------------------------------- Blink Row (Approval Flow) -----------------------------------------------------------------------

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

});

// --------------------------------------------------------- Helpers -----------------------------------------------------------------------

function getQueryParam(param) {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get(param);
}

function validateGroupName(ID) {

    let name = document.getElementById(ID).value;
    let errors = [];

    if (name.length > 90) errors.push("not exceed 90 characters");
    if (name.length < 3) errors.push("be at least 3 characters long");
    if (!/^[a-zA-Z0-9_.]+$/.test(name)) {
        errors.push("contain only letters, numbers, underscores, or dots");
    }

    if (errors.length > 0) {
        return errors.length === 1
            ? `Your group name must ${errors[0]}.`
            : `Your group name must ${errors.slice(0, -1).join(', ')}, and ${errors[errors.length - 1]}.`;
    }

    return "Done";
}
