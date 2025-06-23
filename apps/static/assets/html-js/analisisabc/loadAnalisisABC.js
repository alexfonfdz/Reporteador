import { DataTable } from "../utils/data_table.js"

const getProducts = async (e) => {
    const catalogName = e.target.getAttribute('data-catalog')
    const enterprise = e.target.getAttribute('data-enterprise')

    // Mostrar loader en el modal
    showProductModal('Cargando...', true)

    try {
        const url = new URL('/getProductCatalog', window.location.origin)
        url.searchParams.set('catalog', catalogName)
        url.searchParams.set('enterprise', enterprise)
        url.searchParams.set('per_page', '1000') // traer todos

        const res = await fetch(url)
        const data = await res.json()

        if (!data || !data.data || !Array.isArray(data.data)) {
            showProductModal('No se encontraron productos.', false)
            return
        }

        // Construir tabla HTML
        let html = `<div class="table-responsive"><table class="table table-bordered table-sm"><thead><tr>
            <th>Código</th>
            <th>Descripción</th>
            <th>Familia</th>
            <th>Subfamilia</th>
            <th>Marca</th>
            <th>Catálogo</th>
            <th>Empresa</th>
        </tr></thead><tbody>`

        if (data.data.length === 0) {
            html += `<tr><td colspan="7" class="text-center">No hay productos para este catálogo.</td></tr>`
        } else {
            data.data.forEach(prod => {
                html += `<tr>
                    <td>${prod.code || ''}</td>
                    <td>${prod.description || ''}</td>
                    <td>${prod['family__name'] || ''}</td>
                    <td>${prod['subfamily__name'] || ''}</td>
                    <td>${prod['brand__name'] || ''}</td>
                    <td>${prod['catalog__name'] || ''}</td>
                    <td>${prod.enterprise || ''}</td>
                </tr>`
            })
        }
        html += '</tbody></table></div>'
        showProductModal(html, false)
    } catch (err) {
        showProductModal('Error al obtener productos.', false)
    }
}

// Modal helpers
function ensureProductModal() {
    let modal = document.getElementById('product-modal')
    if (!modal) {
        modal = document.createElement('div')
        modal.id = 'product-modal'
        modal.innerHTML = `
        <div class="modal fade" tabindex="-1" id="product-modal-inner" aria-hidden="true">
          <div class="modal-dialog modal-lg modal-dialog-centered">
            <div class="modal-content">
              <div class="modal-header">
                <h5 class="modal-title">Productos del catálogo</h5>
                <button type="button" class="btn-close" data-dismiss="modal" aria-label="Cerrar">x</button>
              </div>
              <div class="modal-body" id="product-modal-body">
              </div>
            </div>
          </div>
        </div>
        `
        document.body.appendChild(modal)
    }
    return modal
}

function showProductModal(content, isLoading) {
    ensureProductModal()
    const modalBody = document.getElementById('product-modal-body')
    if (modalBody) {
        modalBody.innerHTML = isLoading ? '<div class="text-center my-4"><div class="spinner-border text-primary"></div></div>' : content
    }
    // Mostrar el modal usando Bootstrap 5
    let bsModal = window.bootstrap && window.bootstrap.Modal
        ? window.bootstrap.Modal.getOrCreateInstance(document.getElementById('product-modal-inner'))
        : null
    if (!bsModal) {
        // fallback para Bootstrap 4
        $('#product-modal-inner').modal('show')
    } else {
        bsModal.show()
    }
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

// Preprocesador para colorear celdas según porcentaje (mejor manejo de negativos)
const percentageColorFormatter = ({ currentValue, td }) => {
    if (typeof currentValue !== "number") {
        const num = Number(currentValue)
        if (!isNaN(num)) currentValue = num
        else return currentValue
    }
    if (!td) return currentValue
    if (currentValue < 0) {
        td.style.backgroundColor = "#f8d7da" // rojo claro
    } else if (currentValue >= 0 && currentValue <= 10) {
        td.style.backgroundColor = "#fff3cd" // amarillo claro
    } else if (currentValue > 10) {
        td.style.backgroundColor = "#d4edda" // verde claro
    }
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
        column: "Productos",
        accessorFn: ({ row }) => {        
            return `<button class="btn btn-primary fetchProducts" data-catalog="${row.catalog_name}" data-enterprise="${row.enterprise}">Ver Productos</button>`
        }
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
        preprocess: [percentageColorFormatter, currencyFormatter]
    },
    {
        column: "% Utilidad",
        accessorKey: "profit_percentage",
        preprocess: [percentageColorFormatter, percentageFormatter]
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
        preprocess: [percentageColorFormatter, percentageFormatter]
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
        preprocess: [percentageColorFormatter, percentageFormatter]
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
    {
        column: "Venta U Enero",
        accessorKey: "month_sale_u_january"
    },
    {
        column: "Venta $ Enero",
        accessorKey: "month_sale_p_january",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Enero",
        accessorKey: "inventory_close_u_january"
    },
    {
        column: "Inventario cierre $ Enero",
        accessorKey: "inventory_close_p_january",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Febrero",
        accessorKey: "month_sale_u_february"
    },
    {
        column: "Venta $ Febrero",
        accessorKey: "month_sale_p_february",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Febrero",
        accessorKey: "inventory_close_u_february"
    },
    {
        column: "Inventario cierre $ Febrero",
        accessorKey: "inventory_close_p_february",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Marzo",
        accessorKey: "month_sale_u_march"
    },
    {
        column: "Venta $ Marzo",
        accessorKey: "month_sale_p_march",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Marzo",
        accessorKey: "inventory_close_u_march"
    },
    {
        column: "Inventario cierre $ Marzo",
        accessorKey: "inventory_close_p_march",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Abril",
        accessorKey: "month_sale_u_april"
    },
    {
        column: "Venta $ Abril",
        accessorKey: "month_sale_p_april",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Abril",
        accessorKey: "inventory_close_u_april"
    },
    {
        column: "Inventario cierre $ Abril",
        accessorKey: "inventory_close_p_april",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Mayo",
        accessorKey: "month_sale_u_may"
    },
    {
        column: "Venta $ Mayo",
        accessorKey: "month_sale_p_may",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Mayo",
        accessorKey: "inventory_close_u_may"
    },
    {
        column: "Inventario cierre $ Mayo",
        accessorKey: "inventory_close_p_may",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Junio",
        accessorKey: "month_sale_u_june"
    },
    {
        column: "Venta $ Junio",
        accessorKey: "month_sale_p_june",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Junio",
        accessorKey: "inventory_close_u_june"
    },
    {
        column: "Inventario cierre $ Junio",
        accessorKey: "inventory_close_p_june",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Julio",
        accessorKey: "month_sale_u_july"
    },
    {
        column: "Venta $ Julio",
        accessorKey: "month_sale_p_july",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Julio",
        accessorKey: "inventory_close_u_july"
    },
    {
        column: "Inventario cierre $ Julio",
        accessorKey: "inventory_close_p_july",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Agosto",
        accessorKey: "month_sale_u_august"
    },
    {
        column: "Venta $ Agosto",
        accessorKey: "month_sale_p_august",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Agosto",
        accessorKey: "inventory_close_u_august"
    },
    {
        column: "Inventario cierre $ Agosto",
        accessorKey: "inventory_close_p_august",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Septiembre",
        accessorKey: "month_sale_u_september"
    },
    {
        column: "Venta $ Septiembre",
        accessorKey: "month_sale_p_september",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Septiembre",
        accessorKey: "inventory_close_u_september"
    },
    {
        column: "Inventario cierre $ Septiembre",
        accessorKey: "inventory_close_p_september",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Octubre",
        accessorKey: "month_sale_u_october"
    },
    {
        column: "Venta $ Octubre",
        accessorKey: "month_sale_p_october",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Octubre",
        accessorKey: "inventory_close_u_october"
    },
    {
        column: "Inventario cierre $ Octubre",
        accessorKey: "inventory_close_p_october",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Noviembre",
        accessorKey: "month_sale_u_november"
    },
    {
        column: "Venta $ Noviembre",
        accessorKey: "month_sale_p_november",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Noviembre",
        accessorKey: "inventory_close_u_november"
    },
    {
        column: "Inventario cierre $ Noviembre",
        accessorKey: "inventory_close_p_november",
        preprocess: [currencyFormatter]
    },
    {
        column: "Venta U Diciembre",
        accessorKey: "month_sale_u_december"
    },
    {
        column: "Venta $ Diciembre",
        accessorKey: "month_sale_p_december",
        preprocess: [currencyFormatter]
    },
    {
        column: "Inventario cierre U Diciembre",
        accessorKey: "inventory_close_u_december"
    },
    {
        column: "Inventario cierre $ Diciembre",
        accessorKey: "inventory_close_p_december",
        preprocess: [currencyFormatter]
    },
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
    const fetchProductsButtons = document.querySelectorAll('.fetchProducts')
    
    fetchProductsButtons.forEach(button => {
        button.addEventListener('click', getProducts)
    })
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

yearInput.value = new Date().getFullYear()


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

const handleSubmit = async () => {
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


    if (hasError) {
        return
    }
    showLoader();
    await table.updateSearchState({ filters })
    hideLoader();
}

filtersForm.addEventListener('submit', (e) => {e.preventDefault(); handleSubmit()})
handleSubmit()

