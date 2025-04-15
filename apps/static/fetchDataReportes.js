async function fetchAlmacenes() {
    try {
        const response = await fetch('/getAlmacenes', {
            method: 'GET',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
        });
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching almacenes:', error);
        throw error;
    }
}

async function fetchAlmacenData(almacenId, page = 1) {
    try {
        const response = await fetch(`/getAlmacenData?almacen_id=${almacenId}&page=${page}`, {
            method: 'GET',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
        });
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching almacen data:', error);
        throw error;
    }
}