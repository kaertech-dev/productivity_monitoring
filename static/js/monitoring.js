// Initialize dropdown states on page load
function initializeDropdownStates() {
    const customer = document.getElementById('customer');
    const model = document.getElementById('model');
    const station = document.getElementById('station');
    const customerValue = customer.value;
    const modelValue = model.value;
    
    if (!customerValue) {
        model.innerHTML = '<option value="">Select customer first</option>';
        station.innerHTML = '<option value="">Select customer first</option>';
        model.disabled = station.disabled = true;
        return;
    }
    
    model.disabled = false;
    station.disabled = true;
    station.innerHTML = '<option value="">Select model first</option>';
    
    // Trigger customer change to load appropriate models
    setTimeout(() => {
        customer.dispatchEvent(new Event('change'));
        
        // If model is also pre-selected, trigger model change after models are loaded
        if (modelValue) {
            setTimeout(() => {
                const modelOption = model.querySelector(`option[value="${modelValue}"]`);
                if (modelOption) {
                    model.value = modelValue;
                    model.dispatchEvent(new Event('change'));
                }
            }, 1000);
        }
    }, 100);
}

// Toggle loading state on UI elements
function setLoadingState(isLoading) {
    const opacity = isLoading ? '0.6' : '1';
    const cursor = isLoading ? 'not-allowed' : 'pointer';
    const btnText = isLoading ? 'Loading...' : 'Submit';
    
    [document.getElementById('customer'), document.getElementById('model'), document.getElementById('station')].forEach(el => {
        el.disabled = isLoading;
        el.style.opacity = opacity;
        el.style.cursor = cursor;
    });
    
    const arrowDropdown = document.getElementById('arrowDropdown');
    if (arrowDropdown) {
        arrowDropdown.disabled = isLoading;
        arrowDropdown.style.opacity = opacity;
        arrowDropdown.style.cursor = cursor;
    }
    
    const submitBtn = document.getElementById('filterSubmit');
    submitBtn.disabled = isLoading;
    submitBtn.textContent = btnText;
    
    document.querySelectorAll('#form-container button').forEach(btn => {
        btn.disabled = isLoading;
        btn.textContent = btnText;
    });
    
    const spinner = document.getElementById('spinner');
    if (spinner) spinner.style.display = isLoading ? 'inline-block' : 'none';
}

// Handle dropdown item clicks and auto-hide forms
document.querySelectorAll('.dropdown-item').forEach(item => {
    item.addEventListener('click', function (e) {
        e.preventDefault();
        setTimeout(() => {
            const formContainer = document.getElementById('form-container');
            formContainer.style.display = 'block';
            
            document.querySelectorAll('#form-container form').forEach(form => form.style.display = 'none');
            const selectedForm = document.querySelector(this.getAttribute('data-target'));
            if (selectedForm) selectedForm.style.display = 'block';
        }, 500);
    });
});

// Auto-hide forms when dropdown closes
document.querySelector('.dropdown').addEventListener('hidden.bs.dropdown', function () {
    document.getElementById('form-container').style.display = 'none';
    document.querySelectorAll('#form-container form').forEach(form => form.style.display = 'none');
});

// Date form handlers
const dateFormHandlers = {
    weekform: (input) => {
        const [year, week] = input.split("-W");
        const firstDayOfYear = new Date(year, 0, 1);
        const firstWeekDay = firstDayOfYear.getDay();
        const weekStartOffset = (firstWeekDay <= 4 ? firstWeekDay - 1 : firstWeekDay - 8);
        const weekStart = new Date(firstDayOfYear.getTime() + ((week - 1) * 7 + (1 - weekStartOffset)) * 86400000);
        const startDate = weekStart.toISOString().split('T')[0];
        const endDate = new Date(weekStart);
        endDate.setDate(endDate.getDate() + 6);
        return { startDate, endDate: endDate.toISOString().split('T')[0] };
    },
    monthform: (input) => {
        const [year, month] = input.split("-");
        const start_date = `${year}-${month}-01`;
        const endDate = new Date(year, month, 0).toISOString().split('T')[0];
        return { startDate: start_date, endDate };
    },
    dayform: (input) => {
        const endDate = new Date(input);
        endDate.setDate(endDate.getDate() + 1);
        return { startDate: input, endDate: endDate.toISOString().split('T')[0] };
    }
};

Object.entries(dateFormHandlers).forEach(([formId, handler]) => {
    const form = document.getElementById(formId);
    if (!form) return;
    
    form.addEventListener('submit', function(e) {
        e.preventDefault();
        const inputId = formId === 'rangeform' ? '.action-start' : '#' + formId.replace('form', '');
        const input = formId === 'rangeform' 
            ? document.querySelector('.action-start')?.value
            : document.getElementById(formId.replace('form', '')).value;
        
        if (formId === 'rangeform') {
            const start = document.querySelector('.action-start')?.value;
            const end = document.querySelector('.action-end')?.value;
            if (start && end) window.location.href = `/?start_date=${start}&end_date=${end}`;
        } else if (input) {
            const { startDate, endDate } = handler(input);
            window.location.href = `/?start_date=${startDate}&end_date=${endDate}`;
        }
    });
});

// Navigate to operator activity page
window.operatorAction = (operatorName) => {
    if (operatorName) {
        setLoadingState(true);
        window.location.href = `/operator/${operatorName}`;
    }
};

// Helper functions for URL and API management
const getCurrentDateFilters = () => {
    const urlParams = new URLSearchParams(window.location.search);
    return {
        start_date: urlParams.get('start_date'),
        end_date: urlParams.get('end_date')
    };
};

const buildApiUrl = (baseUrl, customer = null, model = null, station = null) => {
    const params = new URLSearchParams();
    if (customer) params.append('customer', customer);
    if (model) params.append('model', model);
    if (station) params.append('station', station);
    
    const dateFilters = getCurrentDateFilters();
    if (dateFilters.start_date) params.append('start_date', dateFilters.start_date);
    if (dateFilters.end_date) params.append('end_date', dateFilters.end_date);
    
    return `${baseUrl}?${params.toString()}`;
};

// Populate dropdown with options
const populateDropdown = (selectElement, items, placeholder, emptyMessage) => {
    selectElement.innerHTML = `<option value="">${placeholder}</option>`;
    
    if (items && items.length > 0) {
        items.forEach(item => {
            const option = document.createElement('option');
            option.value = option.textContent = item;
            selectElement.appendChild(option);
        });
    } else {
        const option = document.createElement('option');
        option.value = "";
        option.textContent = emptyMessage;
        option.disabled = true;
        option.style.fontStyle = "italic";
        selectElement.appendChild(option);
    }
};

// Handle customer change - Fetch models for selected customer
document.getElementById('customer').addEventListener('change', function () {
    const customer = this.value;
    const modelSelect = document.getElementById('model');
    const stationSelect = document.getElementById('station');

    if (!customer) {
        modelSelect.innerHTML = '<option value="">Select customer first</option>';
        stationSelect.innerHTML = '<option value="">Select customer first</option>';
        modelSelect.disabled = stationSelect.disabled = true;
        return;
    }

    modelSelect.disabled = false;
    stationSelect.disabled = true;
    modelSelect.innerHTML = '<option value="">Loading models...</option>';
    modelSelect.disabled = true;
    
    const apiUrl = buildApiUrl('/api/get-models-stations', customer);
    
    fetch(apiUrl)
        .then(response => {
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            return response.json();
        })
        .then(data => {
            populateDropdown(modelSelect, data.models, "All Models", "No models with data available");
            stationSelect.innerHTML = '<option value="">Select model first</option>';
            stationSelect.disabled = true;
        })
        .catch(error => {
            console.error('Error fetching models:', error);
            modelSelect.innerHTML = '<option value="">Failed to load models</option>';
            stationSelect.innerHTML = '<option value="">Select customer first</option>';
            stationSelect.disabled = true;
        })
        .finally(() => {
            modelSelect.disabled = false;
        });
});

// Handle model change - Fetch stations for selected customer + model
document.getElementById('model').addEventListener('change', function () {
    const customer = document.getElementById('customer').value;
    const model = this.value;
    const stationSelect = document.getElementById('station');

    if (!customer) {
        stationSelect.innerHTML = '<option value="">Select customer first</option>';
        stationSelect.disabled = true;
        return;
    }

    stationSelect.innerHTML = '<option value="">Loading stations...</option>';
    stationSelect.disabled = true;
    
    const apiUrl = buildApiUrl('/api/get-models-stations', customer, model);
    
    fetch(apiUrl)
        .then(response => {
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            return response.json();
        })
        .then(data => {
            populateDropdown(stationSelect, data.stations, "All Stations", `No stations for ${model}`);
        })
        .catch(error => {
            console.error('Error fetching stations:', error);
            stationSelect.innerHTML = '<option value="">Failed to load stations</option>';
        })
        .finally(() => {
            stationSelect.disabled = false;
        });
});

// Submit button handler
document.getElementById("filterSubmit").addEventListener("click", function() {
    setLoadingState(true);
    
    const params = new URLSearchParams();
    const customer = document.getElementById("customer").value;
    const model = document.getElementById("model").value;
    const station = document.getElementById("station").value;
    const dateFilters = getCurrentDateFilters();
    
    if (customer) params.append("customer", customer);
    if (model) params.append("model", model);
    if (station) params.append("station", station);
    if (dateFilters.start_date) params.append("start_date", dateFilters.start_date);
    if (dateFilters.end_date) params.append("end_date", dateFilters.end_date);

    window.location.href = "/?" + params.toString();
});

// Initialize on page load
document.addEventListener('DOMContentLoaded', initializeDropdownStates);
window.addEventListener('load', () => setLoadingState(false));
document.addEventListener('visibilitychange', () => {
    if (!document.hidden) setLoadingState(false);
});

// Trigger CSV download
const triggerCsvDownload = (startDate, endDate) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);

    const customer = document.getElementById('customer').value;
    const model = document.getElementById('model').value;
    const station = document.getElementById('station').value;
    if (customer) params.append('customer', customer);
    if (model) params.append('model', model);
    if (station) params.append('station', station);

    setLoadingState(true);
    fetch(`/api/download_csv?${params.toString()}`)
        .then(response => {
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            return response.blob();
        })
        .then(blob => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'production_data.csv';
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        })
        .catch(error => {
            console.error('Error downloading CSV:', error);
            alert('Failed to download CSV. Please try again.');
        })
        .finally(() => setLoadingState(false));
};