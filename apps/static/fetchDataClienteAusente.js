async function fetchClientesAusentes(page = 1, descripcion = '', dateControl = 'un_mes') {
    try {
        const response = await fetch(`/getClientesAusentes?page=${page}&descripcion=${descripcion}&date-control=${dateControl}`, {
            method: 'GET',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
        });
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching clientes:', error);
        throw error;
    }
}