import { DataTable } from "../utils/data_table.js"

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

const tableColumns = [
    {
        column: "#",
        accessorFn: ({ rowIndex, pagination }) => (rowIndex + 1) + (pagination.page - 1) * 10
    },
    {
        column: "Catálogo",
        accessorKey: "catalog_name"
    },
    {
        column: "Familia",
        accessorKey: "family_name"
    },
    {
        column: "Subfamilia",
        accessorKey: "subfamily_name"
    },
    {
        column: "Empresa",
        accessorKey: "enterprise"
    },
    {
        column: "Año",
        accessorKey: "year"
    },
    {
        column: "Total ventas",
        accessorKey: "total_amount",
        preprocess: [currencyFormatter]
    },
    {
        column: "Unidades vendidas",
        accessorKey: "units_sold"
    },
    {
        column: "Utilidad",
        accessorKey: "profit",
        preprocess: [currencyFormatter]
    },
    {
        column: "% Utilidad",
        accessorKey: "profit_percentage",
        preprocess: [percentageFormatter]
    },
    {
        column: "Inventario cierre (u)",
        accessorKey: "inventory_close_u"
    },
    {
        column: "Inventario cierre ($)",
        accessorKey: "inventory_close_p",
        preprocess: [currencyFormatter]
    },
    {
        column: "ROI mensual",
        accessorKey: "monthly_roi",
        preprocess: [percentageFormatter]
    },
    {
        column: "Promedio ventas mes",
        accessorKey: "sold_average_month",
        preprocess: [currencyFormatter]
    },
    {
        column: "Promedio utilidad mes",
        accessorKey: "profit_average_month",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario actual",
        accessorKey: "actual_inventory",
        preprocess: [currencyFormatter]
    },
    {
        column: "Costo venta promedio",
        accessorKey: "average_selling_cost",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario promedio (u)",
        accessorKey: "inventory_average_u"
    },
    {
        column: "Inventario promedio ($)",
        accessorKey: "inventory_average_p",
        preprocess: [currencyFormatter]
    },
    {
        column: "Días inventario",
        accessorKey: "inventory_days"
    },
    {
        column: "% Ventas",
        accessorKey: "sales_percentage",
        preprocess: [percentageFormatter]
    },
    {
        column: "% Acum. Ventas",
        accessorKey: "acc_sales_percentage",
        preprocess: [percentageFormatter]
    },
    {
        column: "ABC Ventas",
        accessorKey: "sold_abc",
        preprocess: [abcFormatter]
    },
    {
        column: "% Acum. Utilidad",
        accessorKey: "acc_profit_percentage",
        preprocess: [percentageFormatter]
    },
    {
        column: "ABC Utilidad",
        accessorKey: "profit_abc",
        preprocess: [abcFormatter]
    },
    {
        column: "Productos Top",
        accessorKey: "top_products",
        preprocess: [topProductsFormatter]
    }
]

const tableBody = document.getElementById('clientes-table-body')
const tableHead = tableBody.parentElement.children[0]

function getAnalysisABC({ page, year, family, subfamily, enterprise, top_product }) {
    const url = new URL('/getAnalysisABC', window.location.origin)
    if (page)
        url.searchParams.set('page', page)
    if (year)
        url.searchParams.set('year', year)
    if (family)
        url.searchParams.set('family', family)
    if (subfamily)
        url.searchParams.set('subfamily', subfamily)
    if (enterprise)
        url.searchParams.set('enterprise', enterprise)
    if (top_product)
        url.searchParams.set('top_product', top_product)

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
            const pageObj = await getAnalysisABC({ page, ...filters });
            return pageObj;
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
const familyList = document.getElementById('family-list')
const subfamilyList = document.getElementById('subfamily-list')
const enterprisesSelect = document.getElementById('enterprises-filter')
const topProductsFilter = document.getElementById('top_products-filter')
const topProductsList = document.getElementById('top_product-list')
const yearInput = document.getElementById('year-filter')

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
            opt.textContent = subfam
            subfamilyList.appendChild(opt)
            uniqueNames.add(subfam)
    })
}

// Cargar familias según la empresa seleccionada
async function loadFamilies(enterprise) {
    familyList.innerHTML = ''

    const url = new URL('/getFamilies', window.location.origin)
    if(enterprise)
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

    if (enterprise)
        url.searchParams.set('enterprise', enterprise)

    if (family)
        url.searchParams.set('family', family)

    url.searchParams.set('enterprise', enterprise)
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

async function loadTopProducts(enterprise, year, family, subfamily){
    const url = new URL('/getTopProducts', window.location.origin)

    if (enterprise)
        url.searchParams.set('enterprise', enterprise)
    if (year)
        url.searchParams.set('year', year)
    if (family)
        url.searchParams.set('family', family)
    if (subfamily)
        url.searchParams.set('subfamily', subfamily)

    const res = await fetch(url)
    if (!res.ok) return
    const topProducts = await res.json()

    topProducts.forEach(topProduct => {
        const opt = document.createElement('option')
        opt.value = topProduct
        opt.textContent = topProduct
        topProductsList.appendChild(opt)
    })
    
}

// Evento para actualizar familias y subfamilias al cambiar empresa
let enterpriseFilterTimeoutId = null
enterprisesSelect.addEventListener('change', async (e) => {
    const enterprise = e.target.value
    clearTimeout(enterpriseFilterTimeoutId)

    enterpriseFilterTimeoutId = setTimeout(async () => {
        if (!enterprise) {
            await loadAllFamilies()
            await loadAllSubfamilies()
        } else {
            await loadFamilies(enterprise)
            await loadSubfamilies(enterprise, familyFilter.value)
        }
    })

})

let familyFilterTimeoutId = null
familyFilter.addEventListener('input', async (e) => {
    const enterprise = enterprisesSelect.value
    const family = e.target.value

    clearTimeout(familyFilterTimeoutId)

    familyFilterTimeoutId = setTimeout(async () => {
        await loadSubfamilies(enterprise, family)
    }, 500)


})


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
        tableBody.parentElement.parentElement.style.position = 'relative';
        tableBody.parentElement.appendChild(loader);
    }
    loader.style.display = 'flex';
}

function hideLoader() {
    const loader = document.getElementById('abc-loader');
    if (loader) loader.style.display = 'none';
}

// Inicializar datalists al cargar la página
document.addEventListener('DOMContentLoaded', async () => {
    await loadAllFamilies()
    await loadAllSubfamilies()
    await loadTopProducts()
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
    clearError("year-error")
    clearError("family-error")
    clearError("subfamily-error")

    let hasError = false

    // Validar año
    const year = filters.year
    if (!year) {
        const el = document.getElementById("year-error")
        el.textContent = "El año es obligatorio."
        el.style.display = "block"
        hasError = true
    }

    // Validar al menos un filtro de familia o subfamilia
    const family = filters.family
    const subfamily = filters.subfamily
    if (!family && !subfamily) {
        ["family-error", "subfamily-error"].forEach(id => {
            const el = document.getElementById(id)
            el.textContent = "Debes ingresar al menos un filtro en familia o subfamilia."
            el.style.display = "block"
        })
        hasError = true
    }

    if (hasError) {
        return
    }
    showLoader();
    await table.updateSearchState({ filters })
    hideLoader();
})