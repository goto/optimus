"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[8908],{3905:(e,t,n)=>{n.d(t,{Zo:()=>l,kt:()=>d});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var p=r.createContext({}),s=function(e){var t=r.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},l=function(e){var t=s(e.components);return r.createElement(p.Provider,{value:t},e.children)},u="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},g=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,p=e.parentName,l=c(e,["components","mdxType","originalType","parentName"]),u=s(n),g=a,d=u["".concat(p,".").concat(g)]||u[g]||m[g]||i;return n?r.createElement(d,o(o({ref:t},l),{},{components:n})):r.createElement(d,o({ref:t},l))}));function d(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,o=new Array(i);o[0]=g;var c={};for(var p in t)hasOwnProperty.call(t,p)&&(c[p]=t[p]);c.originalType=e,c[u]="string"==typeof e?e:a,o[1]=c;for(var s=2;s<i;s++)o[s]=n[s];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}g.displayName="MDXCreateElement"},8359:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>p,contentTitle:()=>o,default:()=>m,frontMatter:()=>i,metadata:()=>c,toc:()=>s});var r=n(7462),a=(n(7294),n(3905));const i={},o="Managing Project & Namespace",c={unversionedId:"client-guide/managing-project-namespace",id:"client-guide/managing-project-namespace",title:"Managing Project & Namespace",description:"Optimus provides a command to register a new project specified in the client configuration or update if it exists:",source:"@site/docs/client-guide/managing-project-namespace.md",sourceDirName:"client-guide",slug:"/client-guide/managing-project-namespace",permalink:"/optimus/docs/client-guide/managing-project-namespace",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/managing-project-namespace.md",tags:[],version:"current",lastUpdatedBy:"Arinda Arif",lastUpdatedAt:1689670665,formattedLastUpdatedAt:"Jul 18, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Configuration",permalink:"/optimus/docs/client-guide/configuration"},next:{title:"Managing Secrets",permalink:"/optimus/docs/client-guide/managing-secrets"}},p={},s=[],l={toc:s},u="wrapper";function m(e){let{components:t,...n}=e;return(0,a.kt)(u,(0,r.Z)({},l,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"managing-project--namespace"},"Managing Project & Namespace"),(0,a.kt)("p",null,"Optimus provides a command to register a new project specified in the client configuration or update if it exists:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus project register\n")),(0,a.kt)("p",null,"You are also allowed to register the namespace by using the with-namespaces flag:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus project register --with-namespaces\n")),(0,a.kt)("p",null,"You can also check the project configuration that has been registered in your server using:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus project describe\n")))}m.isMDXComponent=!0}}]);