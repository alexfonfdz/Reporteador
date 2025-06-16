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

function getAnalysisABC({ page, year, family, subfamily, enterprise }) {
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
        const pageObj = await getAnalysisABC({ page, ...filters })
        return pageObj
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
table.initialize()

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
const enterprisesSelect = document.getElementById('enterprises-filter')

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
    table.updateSearchState({ filters })
})
