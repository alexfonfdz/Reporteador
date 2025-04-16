/**
 * Ajusta el estado del navbar según el tamaño de la ventana.
 */
function adjustNavbarOnResize() {
    const navbarContent = document.getElementById('navbarContent');
    if (!navbarContent) return; // Verificar que el elemento exista

    // Eliminar la clase 'collapsing' si está presente
    if (navbarContent.classList.contains('collapsing')) {
        navbarContent.classList.remove('collapsing');
        navbarContent.style.height = ''; // Restablecer el estilo de altura
    }

    // Ajustar el estado del navbar según el tamaño de la ventana
    const isLargeScreen = window.innerWidth >= 992;
    if (isLargeScreen) {
        ensureNavbarVisible(navbarContent);
    } else {
        ensureNavbarCollapsed(navbarContent);
    }
}

/**
 * Asegura que el navbar esté visible en pantallas grandes.
 * @param {HTMLElement} navbarContent - Elemento del contenido del navbar.
 */
function ensureNavbarVisible(navbarContent) {
    if (!navbarContent.classList.contains('show')) {
        navbarContent.classList.add('show');
    }
}

/**
 * Asegura que el navbar esté colapsado en pantallas pequeñas.
 * @param {HTMLElement} navbarContent - Elemento del contenido del navbar.
 */
function ensureNavbarCollapsed(navbarContent) {
    if (navbarContent.classList.contains('show')) {
        navbarContent.classList.remove('show');
    }
}

// Detectar cambio de tamaño de la ventana
window.addEventListener('resize', adjustNavbarOnResize);