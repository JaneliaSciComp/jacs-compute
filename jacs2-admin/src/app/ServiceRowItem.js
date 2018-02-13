import React from 'react';
import ReactDOM from 'react-dom';
import TableRow from 'grommet/components/TableRow';
import DeployIcon from 'grommet/components/icons/base/Deploy';
import DatabaseIcon from 'grommet/components/icons/base/Database';

const ServiceRowItem = ({ service }) => (
    <TableRow>
      <td>
        {service.serviceName}
      </td>
      <td>
         <DeployIcon/> 
      </td>
      <td> 
         <DatabaseIcon/>
      </td>
    </TableRow>
)

export default ServiceRowItem