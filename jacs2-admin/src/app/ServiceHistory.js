import React, { Component } from 'react';
import { connect } from 'react-redux';
import Table from 'grommet/components/Table';
import TableHeader from 'grommet/components/TableHeader';
import { fetchServices } from './actions'

class ServiceHistory extends Component {
  componentDidMount() {
      this.props.fetchServices('http://jacs2.int.janelia.org:9000');
  }

  render() {
      return (<Table full={true} scrollable={false} selectable={true}>
          <TableHeader labels={['Name', 'Description']} />
          <tbody>
               
          </tbody>
      </Table>);
  }
}

const mapStateToProps = (state) => {
    return {
        services: state.servicesRegistry.services
    };
};
const mapDispatchToProps = (dispatch) => {
    return {
        fetchServices: (url) => dispatch(fetchServices(url))
    };
};

export default connect(mapStateToProps, mapDispatchToProps)(ServiceHistory);