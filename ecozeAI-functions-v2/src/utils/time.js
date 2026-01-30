function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function getFormattedDate() {
    const date = new Date();
    const day = date.getDate();
    const month = date.toLocaleString('default', { month: 'long' });
    const year = date.getFullYear();

    const getDaySuffix = (day) => {
        if (day > 3 && day < 21) return 'th';
        switch (day % 10) {
            case 1: return "st";
            case 2: return "nd";
            case 3: return "rd";
            default: return "th";
        }
    };

    return `${day}${getDaySuffix(day)} ${month} ${year}`;
}

module.exports = {
    sleep,
    getFormattedDate
};