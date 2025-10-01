function showForm() {
    const value = document.getElementById("filterType").value;
    const forms = ["dayform", "weekform", "monthform", "rangeform"];

    forms.forEach(id => {
        let el = document.getElementById(id);
        if (value + "form" === id) {
            el.classList.add("active");  // show with animation
        } else {
            el.classList.remove("active"); // hide smoothly
        }
    });
}

function showSpinner() {
    document.getElementById('spinner-overlay').style.display = 'flex';
}

function handleDaySubmit(event) {
    showSpinner();
    
    // Get selected date
    const selectedDate = document.getElementById('day_date').value;
    
    // Get today's date in YYYY-MM-DD format
    const today = new Date();
    const todayStr = today.toISOString().split('T')[0];
    
    // If selected date is today, redirect to root without query params
    if (selectedDate === todayStr) {
        event.preventDefault();
        window.location.href = '/';
        return false;
    }
    
    // Otherwise, continue with normal form submission
    return true;
}

function downloadCSV() {
    const params = new URLSearchParams(window.location.search);
    let url = '/download-csv?' + params.toString();
    window.location.href = url;
}