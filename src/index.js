const Apify = require('apify');

async function resolveInBatches(promiseArray, batchLength = 10) {
    const promises = [];
    for (const promiseFunction of promiseArray) {
        promises.push(promiseFunction());
        if (promises.length % batchLength === 0) await Promise.all(promises);
    }
    return Promise.all(promises);
}
Apify.main(async () => {
    const requestQueue = await Apify.openRequestQueue();
    await requestQueue.addRequest({ url: 'https://www.hrs.com/en/', userData: { isHomePage: true } });
    await requestQueue.addRequest({ url: 'https://www.hrs.com/en/hotel/europe/cl-k-1', userData: { isCountryList: true } });
    const crawler = new Apify.CheerioCrawler({
        requestQueue,
        useApifyProxy: true,
        apifyProxyGroups: ['BUYPROXIES94952'],
        minConcurrency: 15,
        maxConcurrency: 40,
        handlePageFunction: async ({ request, $ }) => {
            const { isHomePage, isCity, isCountryList, isCountry, isHotelDetail } = request.userData;
            if (isHomePage) {
                console.log(`Processing home page -  ${request.url}...`);
                // Collecting the worldwide items only since UK is in Europe list
                // and Europe hotels has known static link
                const worldWideFuncs = $('ul[data-slide-pane="Hotels worldwide"] .sw-home-footer-links__link ')
                    .map((i, element) => {
                        const url = $('a', element).attr('href');
                        const userData = {};
                        if (url.includes('/d-')) {
                            userData.isCity = true;
                        } else {
                            userData.isCountryList = true;
                        }

                        return () => requestQueue.addRequest({
                            url: /^\//.test(url) ? `https://www.hrs.com${url}` : url,
                            userData,
                        });
                    });
                await resolveInBatches(Array.from(worldWideFuncs));
            } else if (isCountryList) {
                console.log(`Processing country list page -  ${request.url}...`);
                const countryUrls = $('.sw-multi-column-list .sw-multi-column-list__item').map((i, el) => {
                    const url = $('a', el).attr('href');
                    return () => requestQueue.addRequest({
                        url: /^\//.test(url) ? `https://www.hrs.com${url}` : url,
                        userData: {
                            isCountry: true,
                        },
                    });
                });
                await resolveInBatches(Array.from(countryUrls));
            } else if (isCountry) {
                console.log(`Processing country list page -  ${request.url}...`);
                const topLocationsUrls = $('.sw-hotel-list-topLocation__title__link').map((i, el) => {
                    const url = $(el).attr('href');
                    return () => requestQueue.addRequest({
                        url: /^\//.test(url) ? `https://www.hrs.com${url}` : url,
                        userData: {
                            isCity: true,
                        },
                    });
                });
                await resolveInBatches(Array.from(topLocationsUrls));

                const paginationLinks = $('.sw-hotel-list-pagination ul a').map((i, el) => {
                    const url = $(el).attr('href');
                    return () => requestQueue.addRequest({
                        url: /^\//.test(url) ? `https://www.hrs.com${url}` : url,
                        userData: {
                            isCountry: true,
                        },
                    });
                });

                await resolveInBatches(Array.from(paginationLinks));
            } else if (isCity) {
                console.log(`Processing city page -  ${request.url}...`);

                // Resolve data you have got from the page
                const dataToResolve = $('div.sw-hotel-list > a.sw-hotel-list__link').map((i, el) => {
                    const gtmInfo = JSON.parse($(el).attr('data-gtm-click') || '{}');
                    const hotelData = JSON.parse($(el).find('div.sw-hotel-list__element').attr('data-hotel-item') || '{}');
                    const hotelUrl = $(el).attr('href');
                    const starRating = (($(el).find('div[data-stars] div.sw-hotel__rating').attr('class') || '')
                        .match(/(?<=sw-icon-stars-)([0-5.]+)/) || []).pop();

                    const data = {
                        hrsId: hotelData.id,
                        name: hotelData.name,
                        cleanName: gtmInfo.elementClickName || hotelData.name,

                        addressPostalCode: ((hotelData.address || '').match(/\d{5}/) || []).pop(),
                        addressCity: ((hotelData.address || '').match(/(?<=\d{5}&nbsp;)(.*$)/) || [hotelData.address]).pop(),
                        cityDistance: parseFloat(((hotelData.cityDistance || '').match(/(?<=\: )([0-9.]+ km)/) || []).pop()) || undefined,
                        airportDistance: parseFloat(((hotelData.airportDistance || '').match(/(?<=\: )([0-9.]+ km)/) || []).pop()) || undefined,
                        trainStationDistance: parseFloat(((hotelData.trainStationDistance || '').match(/(?<=\: )([0-9.]+ km)/) || []).pop()) || undefined,
                        lat: (hotelData.geo || {}).lat,
                        lon: (hotelData.geo || {}).lng,

                        reviewRating: parseFloat(hotelData.ratingAverage) || undefined,
                        reviewCount: parseFloat(((hotelData.ratingCount || '').match(/^\d+/) || []).pop()) || undefined,
                        starRating,

                        priceTag: `${hotelData.priceInteger} ${hotelData.priceCurrency}`,
                        thumbUrl: hotelData.thumb,
                        url: /^\//.test(hotelUrl) ? `https://www.hrs.com${hotelUrl}` : hotelUrl,
                        isHotelDetail: true,
                    };
                    return () => requestQueue.addRequest({ url: data.url, userData: data });
                });

                await resolveInBatches(Array.from(dataToResolve));
                // Adding pagination every time - this could be optimized,
                // however requestList has unique keys based on urls so it wont add request two times
                const paginationLinks = $('#pagesListAll li:not(.is-ellipsis)').map((i, el) => {
                    const url = $('a', el).attr('href');
                    return () => (url ? requestQueue.addRequest({
                        url: /^\//.test(url) ? `https://www.hrs.com${url}` : url,
                        userData: {
                            isCity: true,
                        },
                    }) : Promise.resolve());
                });
                await resolveInBatches(Array.from(paginationLinks));
            } else if (isHotelDetail) {
                // TODO: Fill the data for detail
                const data = { ...request.userData };

                await Apify.pushData(data);
            }
        },

        // This function is called if the page processing failed more than maxRequestRetries+1 times.
        handleFailedRequestFunction: async ({ request }) => {
            console.log(`Request ${request.url} failed too many times`);
            await Apify.pushData({
                '#debug': Apify.utils.createRequestDebugInfo(request),
            });
        },
    });

    // Run the crawler and wait for it to finish.
    await crawler.run();

    console.log('Crawler finished. - running the detail crawler');
});
