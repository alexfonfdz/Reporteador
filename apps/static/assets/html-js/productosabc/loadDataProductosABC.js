import { DataTable } from "../utils/data_table.js"





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
    {
        column: "Empresa",
        accessorKey: "enterprise"
    },
    {
        column: "Año",
        accessorKey: "year"
    },
    {
        column: "Porcentaje de ventas",
        accessorKey: "sales_percentage",
        preprocess: [percentageFormatter]
    },
    {
        column: "Porcentaje acumulado de ventas",
        accessorKey: "acc_sales_percentage",
        preprocess: [percentageFormatter]
    },

    {
        column: "Ventas ABC",
        accessorKey: "sold_abc"
    },
    {
        column: "Porcentaje de utilidad",
        accessorKey: "profit_percentage",
        preprocess: [percentageFormatter]
    },
    {
        column: "Porcentaje acumulado de utilidad",
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

    },
    // {
    //     column: "Total importe del año",
    //     accessorKey: "total_importe",
    //     preprocess: [currencyFormatter]
    // },
    // {
    //     column: "Utilidad del año",
    //     accessorKey: "utilidad_cantidad",
    //     preprocess: [currencyFormatter]
    // },
    // {
    //     column: "Porcentaje de utilidad del año",
    //     accessorKey: "utilidad_porcentaje",
    //     preprocess: [percentageFormatter]
    // },
    // {
    //     column: "Unidades vendidas en el año",
    //     accessorKey: "unidades_vendidas"
    // },
    // {
    //     column: "Inventario a cierre del año (unidades)",
    //     accessorKey: "inventario_cierre_cantidad"

    // },
    // {
    //     column: "Inventario a cierre del año (pesos)",
    //     accessorKey: "inventario_cierre_porcentaje",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "ROI Mensual",
    //     accessorKey: "roi_mensual",
    //     preprocess: [percentageFormatter]


    // },
    // {
    //     column: "Venta promedio por mes",
    //     accessorKey: "venta_promedio_mes",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Utilidad promedio por mes",
    //     accessorKey: "utilidad_promedio_mes",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Inventario actual",
    //     accessorKey: "inventario_actual",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Costo de venta promedio",
    //     accessorKey: "costo_venta_promedio",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Inventario promedio (unidades)",
    //     accessorKey: "inventario_promedio_cantidad"

    // },
    // {
    //     column: "Inventario promedio (pesos)",
    //     accessorKey: "inventario_promedio",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Dias inventario",
    //     accessorKey: "dias_inventario"

    // },
    // {
    //     column: "% Ventas",
    //     headerClassNames: "text-nowrap",
    //     accessorKey: "porcentaje_ventas",
    //     classNames: "text-nowrap",
    //     preprocess: [percentageFormatter]



    // },
    // {
    //     column: "% Acumulado Ventas",
    //     headerClassNames: "text-nowrap",
    //     accessorKey: "acumulado_ventas",
    //     classNames: "text-nowrap",
    //     preprocess: [percentageFormatter]



    // },
    // {
    //     column: "ABC Ventas",
    //     headerClassNames: "text-nowrap",
    //     accessorKey: "abc_ventas",
    //     preprocess: [abcFormatter]

    // },
    // {
    //     column: "% Utilidad",
    //     headerClassNames: "text-nowrap",
    //     accessorKey: "porcentaje_utilidad",
    //     classNames: "text-nowrap",
    //     preprocess: [percentageFormatter]



    // },
    // {
    //     column: "% Acumulado Utilidad",
    //     headerClassNames: "text-nowrap",
    //     accessorKey: "porcentaje_acumulado_utilidad",
    //     classNames: "text-nowrap",
    //     preprocess: [percentageFormatter]



    // },
    // {
    //     column: "ABC Utilidad",
    //     accessorKey: "abc_utilidad",
    //     preprocess: [abcFormatter]

    // },
    // {
    //     column: "Productos Top",
    //     accessorKey: "productos_top",
    //     preprocess: [topProductsFormatter]

    // },
    // {
    //     column: "Venta del mes Enero (unidades)",
    //     accessorKey: "ventas_enero_cantidad"
    // },
    // {
    //     column: "Venta del mes Enero (pesos)",
    //     accessorKey: "ventas_enero_pesos",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Inventario a cierre de mes Enero (unidades)",
    //     accessorKey: "inventario_enero_cantidad"

    // },
    // {
    //     column: "Inventario a cierre de mes Enero (pesos)",
    //     accessorKey: "inventario_enero_pesos",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Venta del mes Febrero (unidades)",
    //     accessorKey: "ventas_febrero_cantidad"
    // },
    // {
    //     column: "Venta del mes Febrero (pesos)",
    //     accessorKey: "ventas_febrero_pesos",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Inventario a cierre de mes Febrero (unidades)",
    //     accessorKey: "inventario_febrero_cantidad"

    // },
    // {
    //     column: "Inventario a cierre de mes Febrero (pesos)",
    //     accessorKey: "inventario_febrero_pesos",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Venta del mes Marzo (unidades)",
    //     accessorKey: "ventas_marzo_cantidad"
    // },
    // {
    //     column: "Venta del mes Marzo (pesos)",
    //     accessorKey: "ventas_marzo_pesos",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Inventario a cierre de mes Marzo (unidades)",
    //     accessorKey: "inventario_marzo_cantidad"

    // },
    // {
    //     column: "Inventario a cierre de mes Marzo (pesos)",
    //     accessorKey: "inventario_marzo_pesos",
    //     preprocess: [currencyFormatter]


    // },
    // {
    //     column: "Venta del mes Abril (unidades)",
    //     accessorKey: "ventas_abril_cantidad"
    // },
    // {
    //     column: "Venta del mes Abril (pesos)",
    //     accessorKey: "ventas_abril_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Inventario a cierre de mes Abril (unidades)",
    //     accessorKey: "inventario_abril_cantidad"
    // },
    // {
    //     column: "Inventario a cierre de mes Abril (pesos)",
    //     accessorKey: "inventario_abril_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Venta del mes Mayo (unidades)",
    //     accessorKey: "ventas_mayo_cantidad"
    // },
    // {
    //     column: "Venta del mes Mayo (pesos)",
    //     accessorKey: "ventas_mayo_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Inventario a cierre de mes Mayo (unidades)",
    //     accessorKey: "inventario_mayo_cantidad"
    // },
    // {
    //     column: "Inventario a cierre de mes Mayo (pesos)",
    //     accessorKey: "inventario_mayo_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Venta del mes Junio (unidades)",
    //     accessorKey: "ventas_junio_cantidad"
    // },
    // {
    //     column: "Venta del mes Junio (pesos)",
    //     accessorKey: "ventas_junio_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Inventario a cierre de mes Junio (unidades)",
    //     accessorKey: "inventario_junio_cantidad"
    // },
    // {
    //     column: "Inventario a cierre de mes Junio (pesos)",
    //     accessorKey: "inventario_junio_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Venta del mes Julio (unidades)",
    //     accessorKey: "ventas_julio_cantidad"
    // },
    // {
    //     column: "Venta del mes Julio (pesos)",
    //     accessorKey: "ventas_julio_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Inventario a cierre de mes Julio (unidades)",
    //     accessorKey: "inventario_julio_cantidad"
    // },
    // {
    //     column: "Inventario a cierre de mes Julio (pesos)",
    //     accessorKey: "inventario_julio_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Venta del mes Agosto (unidades)",
    //     accessorKey: "ventas_agosto_cantidad"
    // },
    // {
    //     column: "Venta del mes Agosto (pesos)",
    //     accessorKey: "ventas_agosto_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Inventario a cierre de mes Agosto (unidades)",
    //     accessorKey: "inventario_agosto_cantidad"
    // },
    // {
    //     column: "Inventario a cierre de mes Agosto (pesos)",
    //     accessorKey: "inventario_agosto_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Venta del mes Septiembre (unidades)",
    //     accessorKey: "ventas_septiembre_cantidad"
    // },
    // {
    //     column: "Venta del mes Septiembre (pesos)",
    //     accessorKey: "ventas_septiembre_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Inventario a cierre de mes Septiembre (unidades)",
    //     accessorKey: "inventario_septiembre_cantidad"
    // },
    // {
    //     column: "Inventario a cierre de mes Septiembre (pesos)",
    //     accessorKey: "inventario_septiembre_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Venta del mes Octubre (unidades)",
    //     accessorKey: "ventas_octubre_cantidad"
    // },
    // {
    //     column: "Venta del mes Octubre (pesos)",
    //     accessorKey: "ventas_octubre_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Inventario a cierre de mes Octubre (unidades)",
    //     accessorKey: "inventario_octubre_cantidad"
    // },
    // {
    //     column: "Inventario a cierre de mes Octubre (pesos)",
    //     accessorKey: "inventario_octubre_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Venta del mes Noviembre (unidades)",
    //     accessorKey: "ventas_noviembre_cantidad"
    // },
    // {
    //     column: "Venta del mes Noviembre (pesos)",
    //     accessorKey: "ventas_noviembre_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Inventario a cierre de mes Noviembre (unidades)",
    //     accessorKey: "inventario_noviembre_cantidad"
    // },
    // {
    //     column: "Inventario a cierre de mes Noviembre (pesos)",
    //     accessorKey: "inventario_noviembre_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Venta del mes Diciembre (unidades)",
    //     accessorKey: "ventas_diciembre_cantidad"
    // },
    // {
    //     column: "Venta del mes Diciembre (pesos)",
    //     accessorKey: "ventas_diciembre_pesos",
    //     preprocess: [currencyFormatter]

    // },
    // {
    //     column: "Inventario a cierre de mes Diciembre (unidades)",
    //     accessorKey: "inventario_diciembre_cantidad"
    // },
    // {
    //     column: "Inventario a cierre de mes Diciembre (pesos)",
    //     accessorKey: "inventario_diciembre_pesos",
    //     preprocess: [currencyFormatter]

    // },
]

const tableBody = document.getElementById('clientes-table-body')
const tableHead = tableBody.parentElement.children[0]

function getProductosABC({ page, year, family, subfamily, enterprise }) {
    const url = new URL('/getProductsABC', window.location.origin)


    if (page)
        url.searchParams.set('page', page)
    if (year)
        url.searchParams.set('year', year)
    if (family)
        url.searchParams.set('family', family)
    if (subfamily)
        url.searchParams.set('subfamily', subfamily)

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


const pageItems = document.getElementById('page-items')


const params = new URLSearchParams(window.location.search)
const table = new DataTable({
    columns: tableColumns,
    tbody: tableBody,
    thead: tableHead,
    page: Number(params.get('page')) ?? 0,
    service: async ({ page, filters }) => {
        const pageObj = await getProductosABC({ page, ...filters })
        return pageObj
    }
})
table.setOnPageChange((pagination) => {
    if (!pagination.has_next) {
        document.getElementById('next-page').setAttribute('disabled', true)
    } else {
        document.getElementById('next-page').removeAttribute('disabled')
    }
    if (!pagination.has_prev) {
        document.getElementById('prev-page').setAttribute('disabled', true)
    } else {
        document.getElementById('prev-page').removeAttribute('disabled')
    }

})

table.loadHeaders()
table.initialize()


document.getElementById('next-page').addEventListener('click', async () => {
    await table.updateSearchState({ page: table.getCurrentPage() + 1 })
})
document.getElementById('prev-page').addEventListener('click', async () => {
    await table.updateSearchState({ page: table.getCurrentPage() - 1 })
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
    console.log(filters)

    table.updateSearchState({ filters })
})

