"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[9861],{3905:(e,t,n)=>{n.d(t,{Zo:()=>l,kt:()=>f});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var p=r.createContext({}),s=function(e){var t=r.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},l=function(e){var t=s(e.components);return r.createElement(p.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,p=e.parentName,l=c(e,["components","mdxType","originalType","parentName"]),u=s(n),m=o,f=u["".concat(p,".").concat(m)]||u[m]||d[m]||a;return n?r.createElement(f,i(i({ref:t},l),{},{components:n})):r.createElement(f,i({ref:t},l))}));function f(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,i=new Array(a);i[0]=m;var c={};for(var p in t)hasOwnProperty.call(t,p)&&(c[p]=t[p]);c.originalType=e,c[u]="string"==typeof e?e:o,i[1]=c;for(var s=2;s<a;s++)i[s]=n[s];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},7825:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>p,contentTitle:()=>i,default:()=>d,frontMatter:()=>a,metadata:()=>c,toc:()=>s});var r=n(7462),o=(n(7294),n(3905));const a={},i="Plugin",c={unversionedId:"concepts/plugin",id:"concepts/plugin",title:"Plugin",description:"Optimus can provide support for various data warehouses & any third party system to handle all the data transformations",source:"@site/docs/concepts/plugin.md",sourceDirName:"concepts",slug:"/concepts/plugin",permalink:"/optimus/docs/concepts/plugin",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/plugin.md",tags:[],version:"current",lastUpdatedBy:"Ahmad N. F.",lastUpdatedAt:1733216647,formattedLastUpdatedAt:"Dec 3, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Secret",permalink:"/optimus/docs/concepts/secret"},next:{title:"Replay & Backup",permalink:"/optimus/docs/concepts/replay-and-backup"}},p={},s=[],l={toc:s},u="wrapper";function d(e){let{components:t,...n}=e;return(0,o.kt)(u,(0,r.Z)({},l,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"plugin"},"Plugin"),(0,o.kt)("p",null,"Optimus can provide support for various data warehouses & any third party system to handle all the data transformations\nor movement through plugins. You can bring your own plugin by encapsulating all the logic in a docker container."),(0,o.kt)("p",null,"Currently, plugins can be defined as YAML or binary executables. YAML plugin provides the questionnaire and default\nvalues for job task\u2019s / hook\u2019s creation, as well as defines the image to execute. While a binary plugin, it is\ncomplementing the YAML plugin by providing support for automated dependency resolution."))}d.isMDXComponent=!0}}]);