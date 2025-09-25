//app/static/js/file_download_form.js
document.addEventListener('DOMContentLoaded', function() {
    // Handle dropdown form selection
    document.querySelectorAll('.dropdown-item').forEach(function(item) {
        item.addEventListener('click', function(e) {
            e.preventDefault();
            
            var target = this.getAttribute('data-target');
            var formContainer = document.getElementById('form-container');
            
            // Hide all forms
            formContainer.querySelectorAll('form').forEach(function(form) {
                form.style.display = 'none';
            });
            
            // Show selected form
            var selectedForm = document.querySelector(target);
            if (selectedForm) {
                selectedForm.style.display = 'block';
                formContainer.style.display = 'block';
            }
        });
    });
    
    // Hide form container when clicking outside
    document.addEventListener('click', function(e) {
        var formContainer = document.getElementById('form-container');
        var dropdown = document.getElementById('arrowDropdown');
        
        if (!formContainer.contains(e.target) && !dropdown.contains(e.target)) {
            formContainer.style.display = 'none';
        }
    });
    
    // Show loading spinner on form submit
    document.querySelectorAll('form').forEach(function(form) {
        form.addEventListener('submit', function() {
            document.getElementById('spinner').style.display = 'inline-block';
        });
    });
});