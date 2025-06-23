import '/static/assets/html-js/catalogo/loadExcel.js'

// Agrega el loader HTML al DOM
function showLoader() {
    let loader = document.getElementById('abc-loader');
    if (!loader) {
        loader = document.createElement('div');
        loader.id = 'abc-loader';
        loader.style.position = 'absolute';
        loader.style.top = '0';
        loader.style.left = '0';
        loader.style.width = '100%';
        loader.style.height = '100%';
        loader.style.background = 'rgba(255,255,255,0.7)';
        loader.style.display = 'flex';
        loader.style.alignItems = 'center';
        loader.style.justifyContent = 'center';
        loader.style.zIndex = '1000';
        loader.innerHTML = `<div class="spinner-border text-primary" role="status"><span class="visually-hidden">Cargando...</span></div>`;
    }
    loader.style.display = 'flex';
}

function hideLoader() {
    const loader = document.getElementById('abc-loader');
    if (loader) loader.style.display = 'none';
}


const excelForm = document.getElementById('load-excel-form')
const excelFileInput = document.getElementById('excel-file')

// Mensaje de error/success debajo del input file
let excelMsg = document.createElement('div')
excelMsg.id = 'excel-upload-msg'
excelMsg.style.marginTop = '5px'
excelFileInput.parentElement.appendChild(excelMsg)

excelForm.addEventListener('submit', async (e) => {
    e.preventDefault()
    excelMsg.innerHTML = ''
    excelMsg.className = ''
    const file = excelFileInput.files[0]
    if (!file) {
        excelMsg.innerHTML = 'Selecciona un archivo Excel.'
        excelMsg.className = 'text-danger'
        return
    }
    const formData = new FormData(e.target)
    formData.append('file', file)
    showLoader()
    try {
        const res = await fetch('/uploadCatalogFile', {
            method: 'POST',
            body: formData
        })
        const result = await res.json()
        if (result.success) {
            excelMsg.innerHTML = result.message || 'Archivo cargado correctamente.'
            excelMsg.className = 'text-success'
        } else {
            excelMsg.innerHTML = result.error || 'Error al cargar el archivo.'
            excelMsg.className = 'text-danger'
        }
    } catch (err) {
        excelMsg.innerHTML = 'Error inesperado al cargar el archivo.'
        excelMsg.className = 'text-danger'
    }
    hideLoader()
})