export class DataTable {
    constructor({ columns, tbody, thead, page, filters, service }) {

        this.columns = columns
        this.tbody = tbody
        this.thead = thead
        this.searchState = {
            filters: filters ? filters : {}
        }
        this.pagination = {
            page: page ? page : 1,
            num_pages: 1,
            has_next: true,
            has_prev: false
        }
        this.service = service
    }

    getCurrentPage() { return this.pagination.page }
    getNumPages() { return this.pagination.num_pages }

    setOnPageChange(fun) {
        if (typeof fun != 'function') { return }

        this.onPageChange = fun
    }

    async updateSearchState({ page, filters }) {
        if (page) {
            this.pagination.page = page
        }

        if (filters) {
            this.searchState.filters = filters
            this.pagination.page = 1
        }

        await this.initialize()

    }

    updateURLState() {
        const url = new URL(window.location)

        url.searchParams.set('page', this.pagination.page)

        Object.entries(this.searchState.filters).forEach(entry => {
            if (!entry[1]) { return }

            url.searchParams.set(entry[0], entry[1])
        })

        window.history.pushState({}, '', url)
    }

    async initialize() {
        const { data, pagination } = await this.service({ page: this.pagination.page, filters: this.searchState.filters })

        this.pagination.has_next = pagination.num_pages > pagination.page
        this.pagination.has_prev = pagination.page > 1
        this.pagination.num_pages = pagination.num_pages

        this.tbody.innerHTML = ''
        this.loadData(data)
        this.updateURLState()

        if (this.onPageChange) this.onPageChange(this.pagination)
    }

    loadHeaders() {
        if (!this.columns) {
            console.warn("No columns registered in datatable")
            return
        }

        if (!this.thead) {
            console.warn("No thead registered in datatable")
        }
        this.columns.forEach(tableColumn => {
            let th

            if (!tableColumn.column) {
                return
            }

            switch (typeof tableColumn.column) {
                case 'string':
                    th = document.createElement('th')
                    th.innerHTML = tableColumn.column

                    break;

                case 'function':
                    const returnValue = tableColumn.column()

                    switch (typeof returnValue) {
                        case 'string':
                            th = document.createElement('th')
                            th.innerHTML = tableColumn.column
                            break;

                        case 'object':
                            th = returnValue
                            break
                    }
            }
            if (tableColumn.headerClassNames) {
                th.classList.add(tableColumn.headerClassNames)
            }
            this.thead.appendChild(th)
        })
    }

    loadData(data) {
        const tableData = data

        if (!this.columns) {
            console.warn("No columns registered in datatable")
        }

        tableData.forEach((row, rowIndex) => {
            const tr = document.createElement('tr')

            this.columns.forEach((tableColumn) => {
                const td = document.createElement('td')


                let value
                if (tableColumn.accessorFn && typeof tableColumn.accessorFn == 'function') {
                    value = tableColumn.accessorFn({ row, rowIndex, pagination: this.pagination })
                } else if (tableColumn.accessorKey && typeof tableColumn.accessorKey == 'string') {
                    value = row[tableColumn.accessorKey]
                } else {
                    console.warn(`Column ${tableColumn.column} doesnt have neither a correct accessorFn nor accessorKey`)
                    return
                }

                switch (typeof tableColumn.preprocess) {
                    case "function":
                        value = tableColumn.preprocess({ currentValue: value, td, tr, data: data })

                        break;

                    case "object":
                        if (!Array.isArray(tableColumn.preprocess)) {
                            console.warn(`Preprocess attribute in column ${tableColumn.column} is neither a function nor a function array`)
                            break;
                        }

                        value = tableColumn.preprocess.reduce((acc, curr, index) => {
                            if (typeof curr != "function") {
                                console.warn(`Preprocess element ${index} in the preprocess array for column ${tableColumn.column} is not a function`)
                                return acc
                            }

                            return curr({ currentValue: acc, td: td, tr: tr, data: data })
                        }, value)

                        break;

                }


                if (!value) {
                    td.innerHTML = '--'
                } else {
                    td.innerHTML = value
                }


                if (tableColumn.classNames) {
                    td.className = tableColumn.classNames
                }


                tr.appendChild(td)
            })

            this.tbody.appendChild(tr)

        })
    }
}

