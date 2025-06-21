import { DataTable } from "../utils/data_table.js"



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
        tableBody.parentElement.parentElement.style.position = 'relative';
        tableBody.parentElement.appendChild(loader);
    }
    loader.style.display = 'flex';
}
function hideLoader() {
    const loader = document.getElementById('abc-loader');
    if (loader) loader.style.display = 'none';
}

const currencyFormatter = ({ currentValue }) => {
    let numberValue
    if (typeof currentValue == "string") {
        numberValue = Number(currentValue)
    } else if (typeof currentValue == "number") {
        numberValue = currentValue
    }

    if (!numberValue) {
        return null
    }

    return numberValue.toLocaleString("es-MX", { style: "currency", currency: "MXN" })

}

const percentageFormatter = ({ currentValue }) => {
    let stringValue
    if (typeof currentValue == "string") {
        stringValue = currentValue
    } else if (typeof currentValue == "number") {
        stringValue = currentValue.toFixed(2)
    }

    if (!stringValue) {
        return null
    }

    return `${stringValue}%`

}

const abcFormatter = ({ currentValue, td }) => {

    if (!currentValue || !td) {
        return currentValue
    }

    try {
        switch (currentValue) {
            case 'A':
                td.classList.add('table-success')
                break;
            case 'B':
                td.classList.add('table-info')
                break;
            case 'C':
                td.classList.add('table-warning')
                break;
            default:
                td.classList.add('table-light')
        }
    } catch (e) {
        console.error(e)
    }

    return currentValue
}

const topProductsFormatter = ({ currentValue, td }) => {
    switch (currentValue) {
        case 'AA':
            td.classList.add("table-success")
            return currentValue
    }

    td.classList.add("table-secondary")
    return currentValue
}

function getDynamicColumns(years){
    const allStats = []


    for (const year of years){

        allStats.push({
            column: `Porcentaje de ventas ${year}`,
            accessorFn: ({row}) => {
                return row['stats'][year]['sales_percentage']
            },
            preprocess: [percentageFormatter]
        })

        allStats.push({
            column: `Porcentaje acumulado de ventas ${year}`,
            accessorFn: ({row}) => {
                return row['stats'][year]['acc_sales_percentage']
            },
            preprocess: [percentageFormatter]
        })

        allStats.push({
            column: `Ventas ABC ${year}`,
            accessorFn: ({row}) => {
                return row['stats'][year]['sold_abc']
            },
        })

        allStats.push({
            column: `Porcentaje de utilidad ${year}`,
            accessorFn: ({row}) => {
                return row['stats'][year]['profit_percentage']
            },
            preprocess: [percentageFormatter]
        })

        allStats.push({
            column: `Porcentaje acumulado de utilidad ${year}`,
            accessorFn: ({row}) => {
                return row['stats'][year]['acc_profit_percentage']
            },
            preprocess: [percentageFormatter]
        })

        allStats.push({
            column: `ABC Utilidad ${year}`,
            accessorFn: ({row}) => {
                return row['stats'][year]['profit_abc']
            },
            preprocess: [abcFormatter]
        })

        allStats.push({
            column: `Productos Top ${year}`,
            accessorFn: ({row}) => {
                return row['stats'][year]['top_products']
            },

            preprocess: [topProductsFormatter]
        },)
    }

    return allStats


}


function getTableColumns(years){
    const tableColumns = [
    {
        column: "#",
        accessorFn: ({ rowIndex, pagination }) => (rowIndex + 1) + (pagination.page - 1) * 10
    },
    {
        column: "Código",
        accessorKey: "product_code",

    },

    {
        column: "Familia",
        headerClassNames: "text-nowrap",
        accessorKey: "family_name",
        classNames: "text-nowrap position-sticky"

    },
    {
        column: "Subfamilia",
        headerClassNames: "text-nowrap",
        accessorKey: "subfamily_name",
        classNames: "text-nowrap"

    },
    {
        column: "Marca",
        accessorKey: "brand_name"
    },
    {
        column: "Descripción",
        accessorKey: "product_description",
        classNames: "text-nowrap"
    },
    // Nuevas columnas para los totales por rango de fechas
    {
        column: "Total ventas (rango)",
        accessorKey: "total_amount",
        preprocess: [currencyFormatter]
    },
    {
        column: "Utilidad (rango)",
        accessorKey: "profit",
        preprocess: [currencyFormatter]
    },
    {
        column: "Unidades vendidas (rango)",
        accessorKey: "units_sold"
    },
  ...getDynamicColumns(years)
]

    return tableColumns

}

let tableColumns = getTableColumns([2025])



const tableBody = document.getElementById('clientes-table-body')
const tableHead = tableBody.parentElement.children[0]

function getProductosABC({ page, date_start, date_end, family, subfamily, brand, catalog, enterprise }) {
    const url = new URL('/getProductsABC', window.location.origin)

    if (page)
        url.searchParams.set('page', page)
    if (date_start)
        url.searchParams.set('date_start', date_start)
    if (date_end)
        url.searchParams.set('date_end', date_end)
    if (family)
        url.searchParams.set('family', family)
    if (subfamily)
        url.searchParams.set('subfamily', subfamily)
    if (brand)
        url.searchParams.set('brand', brand)
    if (catalog)
        url.searchParams.set('catalog', catalog)
    if (enterprise)
        url.searchParams.set('enterprise', enterprise)

    url.searchParams.set('per_page', 10)

    return (
        fetch(url,
            {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                }
            }
        )
            .then((response) => {
                if (!response.ok) {
                    throw new Exception('The response went wrong')
                }
                return response.json()
            })
            .then(result => {
                // Adaptar para la nueva estructura { data, pagination }
                if (result && result.data && result.pagination) {
                    return {
                        data: result.data,
                        pagination: result.pagination
                    }
                }
                // fallback para compatibilidad
                return result
            })
            .catch(err => {
                console.log(err)
            })

    )
}

function getEnterprises() {
    return fetch('/getEnterprises')
        .then((response) => {
            if (!response.ok) {
                throw new Exception('The response went wrong')
            }
            return response.json()
        })
        .catch(err => {
            console.log(err)
        })
}




const params = new URLSearchParams(window.location.search)
const table = new DataTable({
    columns: tableColumns,
    tbody: tableBody,
    thead: tableHead,
    page: Number(params.get('page')) ?? 0,
    service: async ({ page, filters }) => {
        showLoader();
        try {
            const pageObj = await getProductosABC({ page, ...filters })
            // Adaptar para la nueva estructura
            if (pageObj && pageObj.data && pageObj.pagination) {
                if (pageObj.data && pageObj.data.length){
                    const years = Object.keys(pageObj.data[0]['stats'])
                    const newColumns = getTableColumns(years)
                    table.columns = newColumns
                    table.loadHeaders() 
                }
                return {
                    data: pageObj.data,
                    pagination: pageObj.pagination
                }
            }
            return pageObj
        } finally {
            hideLoader();
        }
    }
})
table.setOnPageChange((pagination) => {
    if (!pagination.has_next) {
        document.getElementById('next-page').setAttribute('disabled', true)
        document.getElementById('next-page').parentElement.classList.add('disabled')
    } else {
        document.getElementById('next-page').removeAttribute('disabled')
        document.getElementById('next-page').parentElement.classList.remove('disabled')
        
    }
    if (!pagination.has_prev) {
        document.getElementById('previous-page').setAttribute('disabled', true)
        document.getElementById('previous-page').parentElement.classList.add('disabled')
    } else {
        document.getElementById('previous-page').removeAttribute('disabled')
        document.getElementById('previous-page').parentElement.classList.remove('disabled')
    }

    if (pagination.page == 1) {
        document.getElementById('first-page').setAttribute('disabled', true)
        document.getElementById('first-page').parentElement.classList.add('disabled')
    } else {
        document.getElementById('first-page').removeAttribute('disabled')
        document.getElementById('first-page').parentElement.classList.remove('disabled')
    }

    if (pagination.page == pagination.num_pages) {
        document.getElementById('last-page').setAttribute('disabled', true)
        document.getElementById('last-page').parentElement.classList.add('disabled')
    } else {
        document.getElementById('last-page').removeAttribute('disabled')
        document.getElementById('last-page').parentElement.classList.remove('disabled')
    }

    document.getElementById('results').innerHTML = `MOSTRANDO  <span class="text-body-primary">${pagination.page}</span> DE ${pagination.num_pages} PAGINAS`
})

table.loadHeaders()


document.getElementById('next-page').addEventListener('click', async () => {
    await table.updateSearchState({ page: table.getCurrentPage() + 1 })
})
document.getElementById('previous-page').addEventListener('click', async () => {
    await table.updateSearchState({ page: table.getCurrentPage() - 1 })
})

document.getElementById('first-page').addEventListener('click', async () => {
    await table.updateSearchState({ page: 1 })
})
document.getElementById('last-page').addEventListener('click', async () => {
    await table.updateSearchState({ page: table.getNumPages() })
})

const filtersForm = document.getElementById('filter-form')
const familyFilter = document.getElementById('family-filter')
const subfamilyInput = document.getElementById('subfamily-filter')
const brandInput = document.getElementById('brand-filter')
const catalogInput = document.getElementById('catalog-filter')
const familyList = document.getElementById('family-list')
const subfamilyList = document.getElementById('subfamily-list')
const brandList = document.getElementById('brand-list')
const catalogList = document.getElementById('catalog-list')
const enterprisesSelect = document.getElementById('enterprises-filter')
const dateStartInput = document.getElementById('date-start-filter')
const dateEndInput = document.getElementById('date-end-filter')

let minMovementsDate = null;

// Obtener la fecha mínima permitida desde el backend
async function fetchMinMovementsDate() {
    const res = await fetch('/getMinMovementsDate');
    if (!res.ok) return;
    const data = await res.json();
    minMovementsDate = data.min_date;
    if (minMovementsDate) {
        dateStartInput.setAttribute('min', minMovementsDate);
    }
}

// Cargar todas las familias únicas por nombre
async function loadAllFamilies() {
    familyList.innerHTML = ''
    const url = new URL('/getFamilies', window.location.origin)
    const res = await fetch(url)
    if (!res.ok) return
    const families = await res.json()
    const uniqueNames = new Set()
    families.forEach(fam => {
        const opt = document.createElement('option')
        opt.value = fam
        opt.textContent = fam
        familyList.appendChild(opt)
        uniqueNames.add(fam)
    })
}

// Cargar todas las subfamilias únicas por nombre
async function loadAllSubfamilies() {
    subfamilyList.innerHTML = ''
    const url = new URL('/getSubfamilies', window.location.origin)
    const res = await fetch(url)
    if (!res.ok) return
    const subfamilies = await res.json()
    const uniqueNames = new Set()
    subfamilies.forEach(subfam => {
        const opt = document.createElement('option')
        opt.value = subfam
        subfamilyList.appendChild(opt)
        uniqueNames.add(subfam)
    })
}

// Cargar todas las marcas únicas por nombre
async function loadAllBrands() {
    brandList.innerHTML = ''
    const url = new URL('/getBrands', window.location.origin)
    const res = await fetch(url)
    if (!res.ok) return
    const brands = await res.json()
    const uniqueNames = new Set()
    brands.forEach(brand => {
        if (!uniqueNames.has(brand.name)) {
            const opt = document.createElement('option')
            opt.value = brand.name
            brandList.appendChild(opt)
            uniqueNames.add(brand.name)
        }
    })
}

// Cargar todas los catálogos únicos por nombre
async function loadAllCatalogs() {
    catalogList.innerHTML = ''
    const url = new URL('/getCatalogs', window.location.origin)
    const res = await fetch(url)
    if (!res.ok) return
    const catalogs = await res.json()
    const uniqueNames = new Set()
    catalogs.forEach(cat => {
        if (!uniqueNames.has(cat.name)) {
            const opt = document.createElement('option')
            opt.value = cat.name
            catalogList.appendChild(opt)
            uniqueNames.add(cat.name)
        }
    })
}

// Cargar familias según la empresa seleccionada
async function loadFamilies(enterprise) {
    familyList.innerHTML = ''
    if (!enterprise) {
        subfamilyList.innerHTML = ''
        return
    }
    const url = new URL('/getFamilies', window.location.origin)
    url.searchParams.set('enterprise', enterprise)
    const res = await fetch(url)
    if (!res.ok) return
    const families = await res.json()
    families.forEach(fam => {
        const opt = document.createElement('option')
        opt.textContent = fam
        opt.value = fam
        familyList.appendChild(opt)
    })
}

// Cargar subfamilias según la empresa y familia seleccionada
async function loadSubfamilies(enterprise, family) {
    subfamilyList.innerHTML = ''

    const url = new URL('/getSubfamilies', window.location.origin)
    if(enterprise)
        url.searchParams.set('enterprise', enterprise)
    if (family)
        url.searchParams.set('family', family)

    const res = await fetch(url)
    if (!res.ok) return
    const subfamilies = await res.json()
    subfamilies.forEach(subfam => {
        const opt = document.createElement('option')
        opt.value = subfam
        opt.textContent = subfam
        subfamilyList.appendChild(opt)
    })
}

// Cargar marcas según la empresa seleccionada
async function loadBrands(enterprise) {
    brandList.innerHTML = ''
    if (!enterprise) return
    const url = new URL('/getBrands', window.location.origin)
    url.searchParams.set('enterprise', enterprise)
    const res = await fetch(url)
    if (!res.ok) return
    const brands = await res.json()
    brands.forEach(brand => {
        const opt = document.createElement('option')
        opt.value = brand.name
        brandList.appendChild(opt)
    })
}

// Cargar catálogos según la empresa seleccionada
async function loadCatalogs(enterprise) {
    catalogList.innerHTML = ''
    if (!enterprise) return
    const url = new URL('/getCatalogs', window.location.origin)
    url.searchParams.set('enterprise', enterprise)
    const res = await fetch(url)
    if (!res.ok) return
    const catalogs = await res.json()
    catalogs.forEach(cat => {
        const opt = document.createElement('option')
        opt.value = cat.name
        catalogList.appendChild(opt)
    })
}

// Evento para actualizar familias, subfamilias, marcas y catálogos al cambiar empresa
let enterpriseFilterTimeoutId
enterprisesSelect.addEventListener('change', async (e) => {
    const enterprise = e.target.value
    clearTimeout(enterpriseFilterTimeoutId)
    enterpriseFilterTimeoutId = setTimeout(async () => {

        if (!enterprise) {
            await loadAllFamilies()
            await loadAllSubfamilies()
            await loadAllBrands()
            await loadAllCatalogs()
        } else {
            await loadFamilies(enterprise)
            await loadSubfamilies(enterprise, familyFilter.value)
            await loadBrands(enterprise)
            await loadCatalogs(enterprise)
        }
    })
})

let familyFilterTimeoutId = null

function handleFamilyFilter(e) {
    const enterprise = enterprisesSelect.value
    const family = e.target.value
    
    
    if(familyFilterTimeoutId) {
        clearTimeout(familyFilterTimeoutId)
    }
    
    familyFilterTimeoutId = setTimeout(async () => {
        await loadSubfamilies(enterprise, family)
    }, 500)
}

familyFilter.addEventListener('input', handleFamilyFilter)



// Inicializar datalists al cargar la página
document.addEventListener('DOMContentLoaded', async () => {
    await fetchMinMovementsDate();
    await loadAllFamilies()
    await loadAllSubfamilies()
    await loadAllBrands()
    await loadAllCatalogs()
})

getEnterprises().then(enterprises => {
    if (!enterprises) {
        return
    }
    enterprises.forEach(enterprise => {
        enterprisesSelect.appendChild(new Option(enterprise, enterprise))
    })
})



filtersForm.addEventListener('submit', async (e) => {
    e.preventDefault()
    const formData = new FormData(filtersForm)
    const filters = Object.fromEntries(formData.entries())

    // Limpiar errores previos
    function clearError(id) {
        const el = document.getElementById(id)
        if (el) {
            el.style.display = "none"
            el.textContent = ""
        }
    }
    clearError("date-start-error")
    clearError("date-end-error")
    clearError("family-error")
    clearError("subfamily-error")
    clearError("brand-error")
    clearError("catalog-error")
    dateStartInput.classList.remove("is-invalid")

    let hasError = false

    // Validar fechas
    const dateStart = filters.date_start
    const dateEnd = filters.date_end
    if (!dateStart) {
        const el = document.getElementById("date-start-error")
        el.textContent = "La fecha de inicio es obligatoria."
        el.style.display = "block"
        dateStartInput.classList.add("is-invalid")
        hasError = true
    } else if (minMovementsDate && dateStart < minMovementsDate) {
        // Formatear la fecha mínima a formato legible (DD/MM/YYYY)
        const minDateObj = new Date(minMovementsDate)
        const formattedMinDate = minDateObj.toLocaleDateString('es-MX', { year: 'numeric', month: '2-digit', day: '2-digit' })
        const el = document.getElementById("date-start-error")
        el.textContent = `La fecha de inicio debe ser igual o posterior a ${formattedMinDate}.`
        el.style.display = "block"
        dateStartInput.classList.add("is-invalid")
        hasError = true
    } else {
        dateStartInput.classList.remove("is-invalid")
    }
    if (!dateEnd) {
        const el = document.getElementById("date-end-error")
        el.textContent = "La fecha final es obligatoria."
        el.style.display = "block"
        hasError = true
    }

    // Validar al menos un filtro de familia, subfamilia, marca o catálogo
    const family = filters.family
    const subfamily = filters.subfamily
    const brand = filters.brand
    const catalog = filters.catalog
    if (!family && !subfamily && !brand && !catalog) {
        ["family-error", "subfamily-error", "brand-error", "catalog-error"].forEach(id => {
            const el = document.getElementById(id)
            el.textContent = "Debes ingresar al menos un filtro en familia, subfamilia, marca o catálogo."
            el.style.display = "block"
        })
        hasError = true
    }

    if (hasError) {
        return
    }
    await table.updateSearchState({ filters })

})

